{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Cardano.Streamer.Producer where

import Cardano.Crypto.Hash.Class (hashToTextAsHex)
import Cardano.Ledger.Address
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary.Plain (decodeFullDecoder)
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Slotting.EpochInfo.API (epochInfoSlotToUTCTime)
import Cardano.Streamer.Benchmark
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.Ledger
import Cardano.Streamer.LedgerState
import Cardano.Streamer.ProtocolInfo
import Cardano.Streamer.RTS
import Cardano.Streamer.Rewards
import Cardano.Streamer.Time
import Conduit
import Control.Monad.Trans.Except
import Control.ResourceRegistry (withRegistry)
import Control.State.Transition.Extended
import Data.Char (toLower)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Merge.Strict as Map
import qualified Data.Map.Strict as Map
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (configCodec)
import Ouroboros.Consensus.HeaderValidation (
  AnnTip,
  HasAnnTip (..),
  annTipPoint,
  headerStateTip,
 )
import Ouroboros.Consensus.Ledger.Abstract (
  applyBlockLedgerResult,
  reapplyBlockLedgerResult,
 )
import Ouroboros.Consensus.Ledger.Basics (
  ComputeLedgerEvents (..),
  LedgerResult (..),
  applyChainTickLedgerResult,
 )
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState), Ticked)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Shelley.Ledger.Ledger (ShelleyLedgerEvent (..))
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import Ouroboros.Consensus.Storage.Serialisation (DecodeDisk (decodeDisk))
import RIO.Directory (createDirectoryIfMissing, doesDirectoryExist, doesPathExist, listDirectory)
import RIO.File (withBinaryFileDurable)
import RIO.FilePath
import RIO.List as List
import qualified RIO.Set as Set
import qualified RIO.Text as T

sourceBlocks ::
  ( MonadIO m
  , MonadReader (DbStreamerApp blk) m
  , HasHeader blk
  , HasAnnTip blk
  ) =>
  BlockComponent blk b ->
  WithOrigin (AnnTip blk) ->
  ConduitT i (BlockWithInfo b) m ()
sourceBlocks blockComponent withOriginAnnTip = do
  registry <- view registryL
  iDb <- view iDbL
  let blockComponents =
        (,,,) <$> GetSlot <*> GetBlockSize <*> GetHeaderSize <*> blockComponent
  itr <- liftIO $ case withOriginAnnTip of
    Origin ->
      ImmutableDB.streamAll iDb registry blockComponents
    NotOrigin annTip ->
      ImmutableDB.streamAfterKnownPoint iDb registry blockComponents (annTipPoint annTip)
  mStopSlotNo <- dsAppStopSlotNo <$> ask
  case mStopSlotNo of
    Nothing ->
      flip fix 1 $ \loop n ->
        liftIO (ImmutableDB.iteratorNext itr) >>= \case
          ImmutableDB.IteratorExhausted -> pure ()
          ImmutableDB.IteratorResult (slotNo, blockSize, headerSize, comp) -> do
            yield (BlockWithInfo slotNo blockSize headerSize n comp)
            writeStreamerStats slotNo
            loop (n + 1)
    Just stopSlotNo ->
      flip fix 1 $ \loop n ->
        liftIO (ImmutableDB.iteratorNext itr) >>= \case
          ImmutableDB.IteratorExhausted -> pure ()
          ImmutableDB.IteratorResult (slotNo, _, _, _)
            | slotNo > stopSlotNo ->
                logInfo $
                  "Stopped right before processing slot number "
                    <> display slotNo
                    <> " because hard stop was requested at slot number "
                    <> display stopSlotNo
          ImmutableDB.IteratorResult (slotNo, blockSize, headerSize, comp) -> do
            yield (BlockWithInfo slotNo blockSize headerSize n comp)
            writeStreamerStats slotNo
            loop (n + 1)

sourceBlocksWithAccState ::
  ( MonadIO m
  , MonadReader (DbStreamerApp (CardanoBlock StandardCrypto)) m
  ) =>
  BlockComponent (CardanoBlock StandardCrypto) b ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  e ->
  ( ExtLedgerState (CardanoBlock StandardCrypto) ->
    e ->
    BlockWithInfo b ->
    m (ExtLedgerState (CardanoBlock StandardCrypto), e, d)
  ) ->
  ConduitT a d m (ExtLedgerState (CardanoBlock StandardCrypto), e)
sourceBlocksWithAccState blockComponent initState acc0 action = do
  -- Remove any slot numbers that are in the past and sort
  let prepareDiskSnapshots ds =
        sortOn dsNumber $
          case tipSlotNo <$> tipFromExtLedgerState initState of
            Nothing -> ds
            Just (SlotNo initSlotNo) -> List.filter ((>= initSlotNo) . dsNumber) ds
  diskSnapshotsToWrite <- prepareDiskSnapshots . dsAppWriteDiskSnapshots <$> ask
  sourceBlocks blockComponent withOriginAnnTip
    .| loopWithSnapshotWriting initState (acc0, diskSnapshotsToWrite)
  where
    withOriginAnnTip = headerStateTip (headerState initState)
    writeSnapshots extLedgerState = fix $ \go -> \case
      [] -> pure []
      dss@(diskSnapshot : ss)
        | Just tip <- tipFromExtLedgerState extLedgerState
        , -- we use `<=` in order to map slot numbers that don't have blocks into ones that do.
          dsNumber diskSnapshot <= unSlotNo (tipSlotNo tip) ->
            writeExtLedgerState diskSnapshot extLedgerState >> go ss
        | otherwise -> pure dss
    awaitForBlockContinue extLedgerState acc cont = do
      await >>= \case
        Nothing -> pure (extLedgerState, acc)
        Just bwi -> do
          (extLedgerState', acc', c) <- lift $ action extLedgerState acc bwi
          yield c
          cont extLedgerState' acc'
    loopWithSnapshotWriting !extLedgerState (!acc, !dss) =
      writeSnapshots extLedgerState dss >>= \case
        [] -> loopWithoutSnapshotWriting extLedgerState acc
        ss -> awaitForBlockContinue extLedgerState acc $ \extLedgerState' acc' ->
          loopWithSnapshotWriting extLedgerState' (acc', ss)
    loopWithoutSnapshotWriting !extLedgerState !acc =
      awaitForBlockContinue extLedgerState acc loopWithoutSnapshotWriting

sourceBlocksWithState ::
  ( MonadIO m
  , MonadReader (DbStreamerApp (CardanoBlock StandardCrypto)) m
  ) =>
  BlockComponent (CardanoBlock StandardCrypto) b ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  ( ExtLedgerState (CardanoBlock StandardCrypto) ->
    BlockWithInfo b ->
    m (ExtLedgerState (CardanoBlock StandardCrypto), d)
  ) ->
  ConduitT a d m (ExtLedgerState (CardanoBlock StandardCrypto))
sourceBlocksWithState blockComponent initState action =
  fmap fst $
    sourceBlocksWithAccState blockComponent initState () $
      \s a b -> (\(s', c) -> (s', a, c)) <$> action s b

produceLedgerTickEvents ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  (forall proxy era. EraApp era => proxy era -> Event (EraRule "TICK" era) -> e) ->
  ConduitT
    a
    (CardanoEra, EpochNo, SlotNo, Maybe EpochNo, ExtLedgerState (CardanoBlock StandardCrypto), [e])
    (ReaderT (DbStreamerApp (CardanoBlock StandardCrypto)) IO)
    (ExtLedgerState (CardanoBlock StandardCrypto))
produceLedgerTickEvents initExtLedgerState handleTickEvent =
  sourceBlocksWithState GetBlock initExtLedgerState $ \extLedgerState bi -> do
    let extractEvents lr appBlock curSlotNo = do
          let prevEpochNo = extLedgerStateEpochNo extLedgerState
              -- TODO, check whether epoch transition can be reliably extracted from ti
              (_ti, curEpochNo) = tickedExtLedgerStateEpochNo (lrResult lr)
              newEpochNo = guard (curEpochNo /= prevEpochNo) >> Just curEpochNo
              era = getCardanoEra (biBlockComponent bi)
              es = extractLedgerEvents (lrEvents lr) $ \event ->
                case event of
                  ShelleyLedgerEventBBODY _ -> Nothing
                  ShelleyLedgerEventTICK tickEvent -> Just (handleTickEvent event tickEvent)
          newExtLedgerState <- appBlock
          pure (newExtLedgerState, (era, prevEpochNo, curSlotNo, newEpochNo, newExtLedgerState, es))
    advanceBlockGranular (\_ -> pure) extractEvents extLedgerState bi

foldBlocksWithState ::
  ( MonadIO m
  , MonadReader (DbStreamerApp (CardanoBlock StandardCrypto)) m
  ) =>
  BlockComponent (CardanoBlock StandardCrypto) b ->
  -- | Initial ledger state
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  -- | Function to process each block with current ledger state
  ( ExtLedgerState (CardanoBlock StandardCrypto) ->
    BlockWithInfo b ->
    m (ExtLedgerState (CardanoBlock StandardCrypto))
  ) ->
  ConduitT a Void m (ExtLedgerState (CardanoBlock StandardCrypto))
foldBlocksWithState blockComponent initState action =
  sourceBlocksWithState blockComponent initState (\s b -> (,()) <$> action s b)
    `fuseUpstream` sinkNull

-- revalidatePrintBlock
--   :: ExtLedgerState (CardanoBlock StandardCrypto)
--   -> BlockWithInfo (CardanoBlock StandardCrypto)
--   -> RIO
--       (DbStreamerApp (CardanoBlock StandardCrypto))
--       (ExtLedgerState (CardanoBlock StandardCrypto), BlockSummary)
-- revalidatePrintBlock !prevLedger !bwi = do
--   let !block = biBlockComponent bwi
--       !blockSummary = getBlockSummary block
--       EpochNo epochNo = extLedgerStateEpochNo prevLedger
--   when (unSlotNo (bpSlotNo blockSummary) `mod` 100 == 0) $
--     logSticky $
--       "["
--         <> displayShow (bpEra blockSummary)
--         <> " <epoch "
--         <> displayShow epochNo
--         <> ">: "
--         <> displayShow (bpSlotNo blockSummary)
--         <> "]"
--   ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
--   let !result = lrResult $ tickThenReapplyLedgerResult ledgerCfg block prevLedger
--   pure (result, blockSummary)

advanceRawBlockGranular ::
  ( LByteString ->
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (CardanoBlock StandardCrypto) ->
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (CardanoBlock StandardCrypto, a)
  ) ->
  ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    SlotNo ->
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) b
  ) ->
  ( LedgerResult
      (ExtLedgerState (CardanoBlock StandardCrypto))
      (Ticked (ExtLedgerState (CardanoBlock StandardCrypto))) ->
    RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    (a, b) ->
    RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), c)
  ) ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  BlockWithInfo LByteString ->
  RIO
    (DbStreamerApp (CardanoBlock StandardCrypto))
    (ExtLedgerState (CardanoBlock StandardCrypto), c)
advanceRawBlockGranular decodeBlock inspectTickState inspectBlockState prevLedger bwi = do
  ccfg <- configCodec . pInfoConfig . dsAppProtocolInfo <$> ask
  let blockDecoder =
        case decodeFullDecoder "Block" (decodeDisk ccfg) (biBlockComponent bwi) of
          Right decBlock -> pure $ decBlock (biBlockComponent bwi)
          Left err -> throwString $ show err
  (block, a) <- decodeBlock (biBlockComponent bwi) blockDecoder
  let inspectBlockState' tickedExtLedgerState ticker b =
        inspectBlockState tickedExtLedgerState ticker (a, b)
  advanceBlockGranular inspectTickState inspectBlockState' prevLedger (bwi{biBlockComponent = block})

advanceBlockGranular ::
  ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    SlotNo ->
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
  ) ->
  ( LedgerResult
      (ExtLedgerState (CardanoBlock StandardCrypto))
      (Ticked (ExtLedgerState (CardanoBlock StandardCrypto))) ->
    RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    a ->
    RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), b)
  ) ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  BlockWithInfo (CardanoBlock StandardCrypto) ->
  RIO
    (DbStreamerApp (CardanoBlock StandardCrypto))
    (ExtLedgerState (CardanoBlock StandardCrypto), b)
advanceBlockGranular inspectTickState inspectBlockState !prevLedger !bwi = do
  let slotNo = biSlotNo bwi
      block = biBlockComponent bwi
      era = getCardanoEra block
      epochNo = extLedgerStateEpochNo prevLedger
      logStickyStatus = do
        mStopSlotNo <- dsAppStopSlotNo <$> ask
        elapsedTime <- getElapsedTime
        logSticky $
          "["
            <> displayShow era
            <> ": EpochNo "
            <> display (unEpochNo epochNo)
            <> " - "
            <> displayShow slotNo
            <> maybe mempty (\s -> "/" <> display s) mStopSlotNo
            <> "] Blocks: "
            <> display (biBlocksProcessed bwi)
            <> " - Elapsed "
            <> display (T.pack (showTime Nothing False elapsedTime))
  app <- ask
  let ledgerCfg = ExtLedgerCfg . pInfoConfig $ dsAppProtocolInfo app
      tickEvents =
        if null (dsAppRewardsHandles app) then OmitLedgerEvents else ComputeLedgerEvents
      lrTick = applyChainTickLedgerResult tickEvents ledgerCfg slotNo prevLedger
      lrTickResult = lrResult lrTick
      reportException (exc :: SomeException) =
        when (isSyncException exc) $ do
          logStickyStatus
          logError $ "Received an exception: " <> displayShow exc
          reportValidationError exc slotNo block prevLedger
  (blocksToWriteSlotSet, blocksToWriteBlockHashSet) <- readIORef (dsAppWriteBlocks app)
  (extLedgerState, b) <-
    flip withException reportException $ do
      a <- inspectTickState lrTickResult slotNo
      let applyBlockGranular =
            case dsAppValidationMode app of
              FullValidation -> do
                case runExcept (applyBlockLedgerResult OmitLedgerEvents ledgerCfg block lrTickResult) of
                  Right lrBlock -> pure $ lrResult lrBlock
                  Left errorMessage -> do
                    logStickyStatus
                    reportValidationError errorMessage slotNo block prevLedger
              ReValidation ->
                pure $
                  lrResult $
                    reapplyBlockLedgerResult OmitLedgerEvents ledgerCfg block (lrResult lrTick)
              _ -> error "NoValidation is not yet implemeted"
      res <- inspectBlockState lrTick applyBlockGranular a
      when (slotNo `Set.member` blocksToWriteSlotSet) $ do
        atomicModifyIORef' (dsAppWriteBlocks app) $
          \(slotNoSet, blockHashSet) -> ((Set.delete slotNo slotNoSet, blockHashSet), ())
        writeBlockWithState slotNo block prevLedger
      -- TODO: avoid redundant hash calculation
      let blockHash' = rawBlockHash (getRawBlock block)
      when (not (Set.null blocksToWriteBlockHashSet) && blockHash' `Set.member` blocksToWriteBlockHashSet) $ do
        atomicModifyIORef' (dsAppWriteBlocks app) $ \(slotNoSet, blockHashSet) ->
          ((slotNoSet, Set.delete blockHash' blockHashSet), ())
        writeBlockWithState slotNo block prevLedger
      pure res
  when (biBlocksProcessed bwi `mod` 20 == 0) logStickyStatus
  extLedgerState `seq` pure (extLedgerState, b)

reportValidationError ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m, Show a) =>
  a ->
  SlotNo ->
  CardanoBlock c ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  m d
reportValidationError errorMessage slotNo block ledgerState = do
  let rawBlock = getRawBlock block
  logError $
    "Encountered an error while validating a block: " <> display (rawBlockHash rawBlock)
  writeBlockWithState slotNo block ledgerState
  throwString $ show errorMessage

writeBlockWithState ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m) =>
  SlotNo ->
  CardanoBlock c ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  m ()
writeBlockWithState slotNo block ledgerState = do
  let rawBlock = getRawBlock block
      blockHashHex = hashToTextAsHex (extractHash (rawBlockHash rawBlock))
  mOutDir <- dsAppOutDir <$> ask
  forM_ mOutDir $ \outDir -> do
    let prefix = outDir </> show (unSlotNo slotNo) <> "_" <> T.unpack blockHashHex
        mkTxFileName ix = prefix <> "#" <> show ix <.> "cbor"
        fileNameBlock = prefix <.> "cbor"
    writeFileBinary fileNameBlock (rawBlockBytes rawBlock)
    logInfo $ "Written block to: " <> display (T.pack fileNameBlock)
    let epochFileName = outDir </> show (unSlotNo slotNo) <.> "cbor"
    writeNewEpochState epochFileName ledgerState
    logInfo $ "Written NewEpochState to: " <> display (T.pack epochFileName)
    applyBlockTxs
      (liftIO . print)
      (zipWithM_ (\ix -> writeTx (mkTxFileName ix)) [0 :: Int ..])
      block

advanceBlock ::
  ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    -- \^ Intermediate, i.e ticked ledger state
    ExtLedgerState (CardanoBlock StandardCrypto) ->
    -- \^ Next ledger state
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
  ) ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  BlockWithInfo (CardanoBlock StandardCrypto) ->
  RIO
    (DbStreamerApp (CardanoBlock StandardCrypto))
    (ExtLedgerState (CardanoBlock StandardCrypto), a)
advanceBlock inspectBlockState !prevLedger !block =
  advanceBlockGranular
    (\_ _ -> pure ())
    ( \ts getExtLedgerState _ -> do
        s <- getExtLedgerState
        res <- inspectBlockState (lrResult ts) s
        pure (s, res)
    )
    prevLedger
    block

advanceBlock_ ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  BlockWithInfo (CardanoBlock StandardCrypto) ->
  RIO
    (DbStreamerApp (CardanoBlock StandardCrypto))
    (ExtLedgerState (CardanoBlock StandardCrypto))
advanceBlock_ !prevLedger !block =
  fst <$> advanceBlock (\_ _ -> pure ()) prevLedger block

advanceBlockStats ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  BlockWithInfo (CardanoBlock StandardCrypto) ->
  RIO
    (DbStreamerApp (CardanoBlock StandardCrypto))
    (ExtLedgerState (CardanoBlock StandardCrypto), EpochBlockStats)
advanceBlockStats els blk =
  advanceBlock
    ( \tls _ ->
        pure $
          EpochBlockStats
            { ebsEpochNo = snd (tickedExtLedgerStateEpochNo tls)
            , ebsBlockStats =
                case blockLanguageRefScriptsStats tls (biBlockComponent blk) of
                  (refScriptsStats, allRefScriptsStats) ->
                    BlockStats
                      { bsBlocksSize = fromIntegral (biBlockSize blk)
                      , bsScriptsStatsWits = languageStatsTxWits (biBlockComponent blk)
                      , esScriptsStatsOutScripts = languageStatsOutsTxBody (biBlockComponent blk)
                      , esScriptsStatsRefScripts = refScriptsStats
                      , esScriptsStatsAllRefScripts = allRefScriptsStats
                      }
            }
    )
    els
    blk

calcEpochStats ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  ConduitT a c (RIO (DbStreamerApp (CardanoBlock StandardCrypto))) EpochStats
calcEpochStats initLedgerState = do
  epochStats <-
    void (sourceBlocksWithState GetBlock initLedgerState advanceBlockStats)
      .| foldMapC toEpochStats
  writeReport "EpochStats" epochStats
  writeNamedCsv "EpochStats" (epochStatsToNamedCsv epochStats)
  logInfo $ "Final summary: \n    " <> display (fold $ unEpochStats epochStats)
  pure epochStats

data RewardsState = RewardsState
  { curEpoch :: !EpochNo
  , curEpochRewards :: !(Map (Credential 'Staking) Coin)
  , curEpochWithdrawals :: !(Map (Credential 'Staking) Coin)
  }
data RewardsPerEpoch = RewardsPerEpoch
  { rewardsEpochNo :: !EpochNo
  , rewardsReceivedThisEpoch :: !(Map (Credential 'Staking) Coin)
  , rewardsWithdrawnThisEpoch :: !(Map (Credential 'Staking) Coin)
  }

instance Display RewardsPerEpoch where
  display RewardsPerEpoch{..} =
    mconcat $
      [ "Rewards Per Epoch: "
      , display (unEpochNo rewardsEpochNo)
      ]
        ++ [ "\n    "
               <> displayShow cred
               <> ": "
               <> display rew
               <> "  "
               <> display wdrl
           | (cred, (Coin rew, Coin wdrl)) <- Map.toList rewardsAndWithdrawals
           ]
    where
      rewardsAndWithdrawals =
        Map.merge
          (Map.mapMissing (\_ x -> (x, mempty)))
          (Map.mapMissing (\_ x -> (mempty, x)))
          (Map.zipWithMatched (\_ -> (,)))
          rewardsReceivedThisEpoch
          rewardsWithdrawnThisEpoch

accumNewRewards ::
  Set (Credential 'Staking) ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  RewardsState ->
  BlockWithInfo (CardanoBlock StandardCrypto) ->
  ReaderT
    (DbStreamerApp (CardanoBlock StandardCrypto))
    IO
    ( ExtLedgerState (CardanoBlock StandardCrypto)
    , RewardsState
    , Maybe RewardsPerEpoch
    )
accumNewRewards creds prevExtLedgerState rs bwi = do
  let calcRewards tels _els =
        detectNewRewards
          creds
          (curEpoch rs)
          (curEpochRewards rs)
          (curEpochWithdrawals rs)
          tels
  (newExtLedgerState, (newEpochNo, mRewards)) <- advanceBlock calcRewards prevExtLedgerState bwi
  let curBlockWithdrawals = filterBlockWithdrawals creds (biBlockComponent bwi)
      newRecievedRewards = Map.unionWith (<>) (curEpochWithdrawals rs) curBlockWithdrawals
      (newRewardsState, mRewardsPerEpoch) =
        case mRewards of
          Nothing ->
            ( rs
                { curEpoch = newEpochNo
                , curEpochWithdrawals = newRecievedRewards
                }
            , Nothing
            )
          Just (epochRewardsState, epochRecievedRewards) ->
            ( RewardsState
                { curEpoch = newEpochNo
                , curEpochRewards = epochRewardsState
                , -- We include withdrawals from the block, cause it is applied to the state after
                  -- rewards distributions on the epoch boundary
                  curEpochWithdrawals = curBlockWithdrawals
                }
            , Just
                RewardsPerEpoch
                  { rewardsEpochNo = curEpoch rs
                  , rewardsReceivedThisEpoch = epochRecievedRewards
                  , rewardsWithdrawnThisEpoch = curEpochWithdrawals rs
                  }
            )
  forM_ mRewardsPerEpoch (logInfo . display)
  pure (newExtLedgerState, newRewardsState, mRewardsPerEpoch)

-- countTxOuts
--   :: ExtLedgerState (CardanoBlock StandardCrypto)
--   -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) Int
-- countTxOuts initLedgerState = do
--   getSum
--     <$> runConduit
--       ( void (sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock)
--           .| foldMapMC (liftIO . evaluate . foldMap' (fromIntegral . tpOutsCount) . bpTxsSummary)
--       )

-- revalidateWriteNewEpochState
--   :: ExtLedgerState (CardanoBlock StandardCrypto)
--   -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) ()
-- revalidateWriteNewEpochState initLedgerState = do
--   (extLedgerState, mBlockSummary) <-
--     runConduit $
--       sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock
--         `fuseBoth` lastC
--   case mBlockSummary of
--     Nothing -> logError "No blocks where discovered on chain"
--     Just lastBlockSummary -> do
--       mDir <- dsAppOutDir <$> ask
--       forM_ mDir $ \dir -> do
--         let slotNo = unSlotNo (bpSlotNo lastBlockSummary)
--             filePath = dir </> "new-epoch-state_" ++ show slotNo ++ ".cbor"
--         writeNewEpochState filePath extLedgerState

replayChain ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
replayChain initLedgerState = do
  runConduit $ foldBlocksWithState GetBlock initLedgerState advanceBlock_

replayChainWithReport ::
  e ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  ( e ->
    ExtLedgerState (CardanoBlock StandardCrypto) ->
    -- \^ Starting ledger state
    SlotNo ->
    -- \^ New slot number
    Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    -- \^ Ticked ledger state
    BlockWithInfo (CardanoBlock StandardCrypto) ->
    -- \^ Block applied
    ExtLedgerState (CardanoBlock StandardCrypto) ->
    -- \^ New ledger state
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) e
  ) ->
  RIO (DbStreamerApp (CardanoBlock StandardCrypto)) e
replayChainWithReport initAccState initExtLedgerState action =
  snd
    <$> runConduit
      ( sourceBlocksWithInfo
          initAccState
          initExtLedgerState
          ( \accState extLedgerState slotNo tickExtLedgerState blockWithInfo nextExtLedgerState -> do
              (,()) <$> action accState extLedgerState slotNo tickExtLedgerState blockWithInfo nextExtLedgerState
          )
          `fuseUpstream` sinkNull
      )

sourceBlocksWithInfo ::
  -- | Initial accumulator
  e ->
  -- | Initial ledger state
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  ( e ->
    -- \^ Accumulator
    ExtLedgerState (CardanoBlock StandardCrypto) ->
    -- \^ Starting ledger state
    SlotNo ->
    -- \^ New slot number
    Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) ->
    -- \^ Ticked ledger state
    BlockWithInfo (CardanoBlock StandardCrypto) ->
    -- \^ Block applied
    ExtLedgerState (CardanoBlock StandardCrypto) ->
    -- \^ New ledger state
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (e, a)
  ) ->
  ConduitT
    c
    a
    (RIO (DbStreamerApp (CardanoBlock StandardCrypto)))
    (ExtLedgerState (CardanoBlock StandardCrypto), e)
sourceBlocksWithInfo initAccState initExtLedgerState action = do
  sourceBlocksWithAccState
    GetBlock
    initExtLedgerState
    initAccState
    ( \extLedgerState accState blockWithInfo -> do
        (nextExtLedgerState, (newAcc, newYield)) <-
          advanceBlockGranular
            (\_ -> pure)
            ( \lrTick getExtLedgerState slotNo -> do
                nextExtLedgerState <- getExtLedgerState
                res <-
                  action
                    accState
                    extLedgerState
                    slotNo
                    (lrResult lrTick)
                    blockWithInfo
                    nextExtLedgerState
                pure (nextExtLedgerState, res)
            )
            extLedgerState
            blockWithInfo
        pure (nextExtLedgerState, newAcc, newYield)
    )

exportRewards ::
  Set (Credential 'Staking) ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  RIO (DbStreamerApp (CardanoBlock StandardCrypto)) ()
exportRewards creds initExtLedgerState = do
  app <- ask
  let exportRewardsToHandle (era, _prevEpochNo, slotNo, mNewEpochNo, newExtLedgerState, rewards) = do
        -- Keep only non-zero reward distributions
        let nonZeroRewards = Map.filter (/= mempty) rewards
            -- Decidig factor for producing rewards
            newEpochNoWithGlobals = do
              -- only on the epoch boundary
              newEpochNo <- mNewEpochNo
              -- only when there are non-zero rewards
              guard (not (Map.null nonZeroRewards))

              globals <-
                globalsFromLedgerConfig era newExtLedgerState $ pInfoConfig $ dsAppProtocolInfo app
              pure (newEpochNo, globals)
        forM_ newEpochNoWithGlobals $ \(newEpochNo, globals) -> do
          case epochInfoSlotToUTCTime (epochInfo globals) (systemStart globals) slotNo of
            Left err ->
              logError $ "Could not convert slot " <> display slotNo <> " to UTCTime: " <> display err
            Right utcTime -> do
              let rewardsWithHandles =
                    [ (hdl, r)
                    | (cred, hdl) <-
                        Map.toList (dsAppRewardsHandles app)
                    , Just r <- [Map.lookup cred rewards]
                    ]
              forM_ rewardsWithHandles $ \(hdl, r) ->
                writeRewardDistribution hdl $
                  RewardDistribution
                    { rdEra = era
                    , rdEpochNo = newEpochNo
                    , rdSlotNo = slotNo
                    , rdUTCTime = utcTime
                    , rdRewardAmount = r
                    }
              logInfo $ "Written rewards distributed at the beginning of epoch " <> display newEpochNo
  runConduit $
    void (produceLedgerTickEvents initExtLedgerState getRewardsFromEvents)
      .| mapC (fmap ((`Map.restrictKeys` creds) . Map.unionsWith (<>)))
      .| mapM_C exportRewardsToHandle

computeRewards ::
  Set (Credential 'Staking) ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  RIO (DbStreamerApp (CardanoBlock StandardCrypto)) [RewardsPerEpoch]
computeRewards creds initLedgerState = do
  -- (extLedgerState, rs) <-
  runConduit $
    void
      ( sourceBlocksWithAccState GetBlock initLedgerState emptyRewardsState $
          accumNewRewards creds
      )
      .| concatMapC id
      .| sinkList
  where
    --   `fuseBoth` lastC
    -- pure $ fromMaybe [] rs

    emptyRewardsState = RewardsState (EpochNo 0) mempty mempty

replayBenchmarkReport ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  RIO (DbStreamerApp (CardanoBlock StandardCrypto)) StatsReport
replayBenchmarkReport initLedgerState = do
  report <- runConduit $ void (replayWithBenchmarking initLedgerState) .| calcStatsReport
  report <$ writeReport "Benchmark" report

replayWithBenchmarking ::
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  ConduitT
    a
    Stat
    (RIO (DbStreamerApp (CardanoBlock StandardCrypto)))
    (ExtLedgerState (CardanoBlock StandardCrypto))
replayWithBenchmarking initLedgerState = do
  withBenchmarking $ \decodeBlock benchRunTick benchRunBlock ->
    sourceBlocksWithState GetRawBlock initLedgerState $ \ls ->
      advanceRawBlockGranular (const decodeBlock) (benchRunTick ls) (const benchRunBlock) ls

runApp :: Opts -> IO ()
runApp Opts{..} = do
  -- Consensus code will initialize the chain directory if it doesn't exist, which makes
  -- no sense for a tool that is suppose to be read only.
  unlessM (doesDirectoryExist oChainDir) $ do
    throwString $ "Chain directory does not exist: " <> oChainDir
  whenM (null <$> listDirectory oChainDir) $ do
    throwString $ "Chain directory is empty: " <> oChainDir
  logOpts <- logOptionsHandle stdout oVerbose
  withLogFunc (setLogMinLevel oLogLevel $ setLogUseLoc oDebug logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let (writeBlockSlots, writeBlockHashes) =
            bimap Set.fromList Set.fromList $
              partitionEithers $
                map unBlockHashOrSlotNo oWriteBlocks
          appConf =
            AppConfig
              { appConfChainDir = oChainDir
              , appConfFilePath = oConfigFilePath
              , appConfReadDiskSnapshot =
                  DiskSnapshot <$> oReadSnapShotSlotNumber <*> pure oSnapShotSuffix
              , appConfWriteDiskSnapshots =
                  DiskSnapshot <$> oWriteSnapShotSlotNumbers <*> pure oSnapShotSuffix
              , appConfStopSlotNumber = oStopSlotNumber
              , appConfValidationMode = oValidationMode
              , appConfWriteBlocksSlotNoSet = writeBlockSlots
              , appConfWriteBlocksBlockHashSet = writeBlockHashes
              , appConfLogFunc = logFunc
              , appConfRegistry = registry
              }
      void $ runRIO appConf $ runDbStreamerApp $ \initLedger -> do
        forM_ oOutDir (createMissingDirectory "output")
        rtsStatsFilePathMaybe <-
          forM oRTSStatsFilePath $ \rtsStatsFilePath -> do
            -- We need to fail when stats are not enabled before we start creating files
            checkRTSStatsEnabled
            fullPath <-
              if isAbsolute rtsStatsFilePath
                then
                  pure rtsStatsFilePath
                else case oOutDir of
                  Nothing ->
                    throwString $
                      "Supplied a relative path for RTS stats: '"
                        <> rtsStatsFilePath
                        <> "' without supplying the OUT_DIR"
                  Just outDir -> pure $ outDir </> rtsStatsFilePath
            createMissingDirectory "RTS Stats File" (dropFileName fullPath)
            whenM (doesPathExist fullPath) $
              throwString $
                "Can't use an existing file for writing RTS stats: " <> fullPath
            let ext = toLower <$> takeExtension fullPath
            unless (ext == ".csv") $
              logWarn $
                "Expected a file path for a CSV file, but "
                  <> displayShow fullPath
                  <> " has an unexpected extension: "
                  <> displayShow ext
            pure fullPath
        unless (null writeBlockSlots) $
          logInfo $
            "Will try to dump blocks if they exist at slots: "
              <> mconcat (intersperse "," (map display (Set.toList writeBlockSlots)))
        unless (null writeBlockHashes) $
          logInfo $
            "Will try to dump blocks with hashes if they exist: "
              <> mconcat
                (intersperse "," (map display (Set.toList writeBlockHashes)))
        app <- ask
        withMaybeFile rtsStatsFilePathMaybe $ \rtsStatsHandle ->
          runRIO (app{dsAppOutDir = oOutDir, dsAppRTSStatsHandle = rtsStatsHandle}) $ do
            logInfo $ "Starting to " <> display oCommand
            writeStreamerHeader
            case oCommand of
              Replay -> void $ replayChain initLedger
              Benchmark -> void $ replayBenchmarkReport initLedger
              Stats -> void $ runConduit $ calcEpochStats initLedger
              ComputeRewards rewardAccounts -> do
                case oOutDir of
                  Nothing ->
                    logError "Output directory is required for exporting rewards"
                  Just outDir -> do
                    let filePaths =
                          [ ( raCredential rewardAccount
                            , outDir </> T.unpack (formatRewardAccount rewardAccount) <.> "csv"
                            )
                          | rewardAccount <- NE.toList rewardAccounts
                          ]
                        stakingCredentialSet =
                          Set.fromList $ map raCredential $ NE.toList rewardAccounts
                    withFiles filePaths $ \hdls ->
                      local (\env -> env{dsAppRewardsHandles = hdls}) $ do
                        writeRewardsHeaders
                        exportRewards stakingCredentialSet initLedger
  where
    withMaybeFile mFilePath action =
      case mFilePath of
        Nothing -> action Nothing
        Just fp -> withBinaryFileDurable fp WriteMode (action . Just)
    withFiles filePaths action = go [] filePaths
      where
        go hdls [] = action $ Map.fromList hdls
        go hdls ((ra, fp) : fps) =
          withBinaryFileDurable fp WriteMode $ \hdl -> do
            logInfo $ "Opened file for exporting rewards " <> displayShow fp
            go ((ra, hdl) : hdls) fps
    createMissingDirectory name dir =
      unlessM (doesDirectoryExist dir) $ do
        logInfo $ "Creating " <> name <> " directory: " <> displayShow dir
        createDirectoryIfMissing True dir

-- -- TxOuts:
-- total <- runRIO (app{dsAppOutDir = mOutDir}) $ countTxOuts initLedger
-- logInfo $ "Total TxOuts: " <> displayShow total
-- runRIO (app{dsAppOutDir = mOutDir}) $ revalidateWriteNewEpochState initLedger
