{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Cardano.Streamer.Producer where

import Cardano.Crypto.Hash.Class (hashToTextAsHex)
import Cardano.Ledger.Binary.Plain (decodeFullDecoder)
import Cardano.Ledger.Coin
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.SafeHash (extractHash)
import Cardano.Slotting.Slot (WithOrigin (..))
import Cardano.Streamer.Benchmark
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState
import Cardano.Streamer.ProtocolInfo
import Cardano.Streamer.Time
import Conduit
import Control.Monad.Trans.Except
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Merge.Strict as Map
import qualified Data.Map.Strict as Map
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (configCodec)
import Ouroboros.Consensus.HeaderValidation (
  AnnTip (annTipSlotNo),
  HasAnnTip (..),
  annTipPoint,
  headerStateTip,
 )
import Ouroboros.Consensus.Ledger.Abstract (
  applyBlockLedgerResult,
  reapplyBlockLedgerResult,
 )
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (lrResult), applyChainTickLedgerResult)
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState), Ticked)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Protocol.Abstract (ChainDepState)
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import Ouroboros.Consensus.Storage.Serialisation (DecodeDisk (decodeDisk), EncodeDisk)
import Ouroboros.Consensus.Util.ResourceRegistry
import RIO.FilePath
import RIO.List as List
import qualified RIO.Set as Set
import qualified RIO.Text as T

sourceBlocks
  :: ( MonadIO m
     , MonadReader (DbStreamerApp blk) m
     , HasHeader blk
     , HasAnnTip blk
     )
  => BlockComponent blk b
  -> WithOrigin (AnnTip blk)
  -> ConduitT i (BlockWithInfo b) m ()
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
          ImmutableDB.IteratorResult (slotNo, blockSize, headerSize, comp) ->
            yield (BlockWithInfo slotNo blockSize headerSize n comp) >> loop (n + 1)
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
          ImmutableDB.IteratorResult (slotNo, blockSize, headerSize, comp) ->
            yield (BlockWithInfo slotNo blockSize headerSize n comp) >> loop (n + 1)

sourceBlocksWithAccState
  :: ( MonadIO m
     , MonadReader (DbStreamerApp blk) m
     , HasHeader blk
     , HasAnnTip blk
     , EncodeDisk blk (AnnTip blk)
     , EncodeDisk blk (LedgerState blk)
     , EncodeDisk blk (ChainDepState (BlockProtocol blk))
     )
  => BlockComponent blk b
  -> ExtLedgerState blk
  -> e
  -> (ExtLedgerState blk -> e -> BlockWithInfo b -> m (ExtLedgerState blk, e, c))
  -> ConduitT a c m (ExtLedgerState blk, e)
sourceBlocksWithAccState blockComponent initState acc0 action = do
  let prepareDiskSnapshots ds =
        sortOn dsNumber $
          case annTipSlotNo <$> headerStateTip (headerState initState) of
            Origin -> ds
            At (SlotNo curSlotNo) -> List.filter ((> curSlotNo) . dsNumber) ds
  diskSnapshotsToWrite <- prepareDiskSnapshots . dsAppWriteDiskSnapshots <$> ask
  sourceBlocks blockComponent withOriginAnnTip
    .| loopWithSnapshotWriting initState (acc0, diskSnapshotsToWrite)
  where
    withOriginAnnTip = headerStateTip (headerState initState)
    writeSnapshots ledgerState curSlotNo = fix $ \go -> \case
      [] -> pure []
      dss@(s : ss)
        | dsNumber s <= curSlotNo -> writeLedgerState s ledgerState >> go ss
        | otherwise -> pure dss
    loopWithSnapshotWriting !ledgerState (!acc, !dss) =
      await >>= \case
        Nothing -> pure (ledgerState, acc)
        Just bwi -> do
          (ledgerState', acc', c) <- lift $ action ledgerState acc bwi
          yield c
          let SlotNo curSlotNo = biSlotNo bwi
          writeSnapshots ledgerState' curSlotNo dss >>= \case
            [] -> loopWithoutSnapshotWriting ledgerState' acc'
            ss -> loopWithSnapshotWriting ledgerState' (acc', ss)
    loopWithoutSnapshotWriting !ledgerState !acc =
      await >>= \case
        Nothing -> pure (ledgerState, acc)
        Just bwi -> do
          (ledgerState', acc', c) <- lift $ action ledgerState acc bwi
          yield c
          loopWithoutSnapshotWriting ledgerState' acc'

sourceBlocksWithState
  :: ( MonadIO m
     , MonadReader (DbStreamerApp blk) m
     , HasHeader blk
     , HasAnnTip blk
     , EncodeDisk blk (AnnTip blk)
     , EncodeDisk blk (LedgerState blk)
     , EncodeDisk blk (ChainDepState (BlockProtocol blk))
     )
  => BlockComponent blk b
  -> ExtLedgerState blk
  -> (ExtLedgerState blk -> BlockWithInfo b -> m (ExtLedgerState blk, c))
  -> ConduitT a c m (ExtLedgerState blk)
sourceBlocksWithState blockComponent initState action =
  fmap fst $
    sourceBlocksWithAccState blockComponent initState () $
      \s a b -> (\(s', c) -> (s', a, c)) <$> action s b

foldBlocksWithState
  :: ( MonadIO m
     , MonadReader (DbStreamerApp blk) m
     , HasHeader blk
     , HasAnnTip blk
     , EncodeDisk blk (AnnTip blk)
     , EncodeDisk blk (LedgerState blk)
     , EncodeDisk blk (ChainDepState (BlockProtocol blk))
     )
  => BlockComponent blk b
  -> ExtLedgerState blk
  -- ^ Initial ledger state
  -> (ExtLedgerState blk -> BlockWithInfo b -> m (ExtLedgerState blk))
  -- ^ Function to process each block with current ledger state
  -> ConduitT a Void m (ExtLedgerState blk)
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

advanceRawBlockGranular
  :: ( LByteString
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (CardanoBlock StandardCrypto)
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (CardanoBlock StandardCrypto, a)
     )
  -> ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> SlotNo
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) b
     )
  -> ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> RIO
            (DbStreamerApp (CardanoBlock StandardCrypto))
            (ExtLedgerState (CardanoBlock StandardCrypto))
       -> (a, b)
       -> RIO
            (DbStreamerApp (CardanoBlock StandardCrypto))
            (ExtLedgerState (CardanoBlock StandardCrypto), c)
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> BlockWithInfo LByteString
  -> RIO
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

advanceBlockGranular
  :: ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> SlotNo
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> RIO
            (DbStreamerApp (CardanoBlock StandardCrypto))
            (ExtLedgerState (CardanoBlock StandardCrypto))
       -> a
       -> RIO
            (DbStreamerApp (CardanoBlock StandardCrypto))
            (ExtLedgerState (CardanoBlock StandardCrypto), b)
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> BlockWithInfo (CardanoBlock StandardCrypto)
  -> RIO
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
      lrTick = applyChainTickLedgerResult ledgerCfg slotNo prevLedger
      lrTickResult = lrResult lrTick
      reportException (exc :: SomeException) =
        when (isSyncException exc) $ do
          logStickyStatus
          logError $ "Received an exception: " <> displayShow exc
          reportValidationError exc slotNo block prevLedger
  blocksToWriteSlotSet <- readIORef (dsAppWriteBlocks app)
  (extLedgerState, b) <-
    flip withException reportException $ do
      a <- inspectTickState lrTickResult slotNo
      let applyBlockGranular =
            case dsAppValidationMode app of
              FullValidation -> do
                case runExcept (applyBlockLedgerResult ledgerCfg block lrTickResult) of
                  Right lrBlock -> pure $ lrResult lrBlock
                  Left errorMessage -> do
                    logStickyStatus
                    reportValidationError errorMessage slotNo block prevLedger
              ReValidation ->
                pure $ lrResult $ reapplyBlockLedgerResult ledgerCfg block (lrResult lrTick)
              _ -> error "NoValidation is not yet implemeted"
      res <- inspectBlockState lrTickResult applyBlockGranular a
      when (slotNo `Set.member` blocksToWriteSlotSet) $ do
        writeIORef (dsAppWriteBlocks app) (Set.delete slotNo blocksToWriteSlotSet)
        writeBlockWithState slotNo block prevLedger
      pure res
  when (biBlocksProcessed bwi `mod` 20 == 0) logStickyStatus
  extLedgerState `seq` pure (extLedgerState, b)

reportValidationError
  :: (MonadReader (DbStreamerApp blk) m, MonadIO m, Show a, Crypto c)
  => a
  -> SlotNo
  -> CardanoBlock c
  -> ExtLedgerState (CardanoBlock c)
  -> m d
reportValidationError errorMessage slotNo block ledgerState = do
  let rawBlock = getRawBlock block
      blockHashHex = hashToTextAsHex (extractHash (rawBlockHash rawBlock))
  logError $
    "Encountered an error while validating a block: " <> display blockHashHex
  writeBlockWithState slotNo block ledgerState
  throwString $ show errorMessage

writeBlockWithState
  :: (MonadReader (DbStreamerApp blk) m, MonadIO m, Crypto c)
  => SlotNo
  -> CardanoBlock c
  -> ExtLedgerState (CardanoBlock c)
  -> m ()
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

advanceBlock
  :: ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -- \^ Intermediate, i.e ticked ledger state
       -> ExtLedgerState (CardanoBlock StandardCrypto)
       -- \^ Next ledger state
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> BlockWithInfo (CardanoBlock StandardCrypto)
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), a)
advanceBlock inspectBlockState !prevLedger !block =
  advanceBlockGranular
    (\_ _ -> pure ())
    ( \ts getExtLedgerState _ -> do
        s <- getExtLedgerState
        res <- inspectBlockState ts s
        pure (s, res)
    )
    prevLedger
    block

advanceBlock_
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> BlockWithInfo (CardanoBlock StandardCrypto)
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto))
advanceBlock_ !prevLedger !block =
  fst <$> advanceBlock (\_ _ -> pure ()) prevLedger block

advanceBlockStats
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> BlockWithInfo (CardanoBlock StandardCrypto)
  -> RIO
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

calcEpochStats
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> ConduitT
      a
      c
      (RIO (DbStreamerApp (CardanoBlock StandardCrypto)))
      EpochStats
calcEpochStats initLedgerState = do
  epochStats <-
    void (sourceBlocksWithState GetBlock initLedgerState advanceBlockStats)
      .| foldMapC toEpochStats
  writeReport "EpochStats" epochStats
  writeNamedCsv "EpochStats" (epochStatsToNamedCsv epochStats)
  logInfo $ "Final summary: \n    " <> display (fold $ unEpochStats epochStats)
  pure epochStats

data RewardsState c = RewardsState
  { curEpoch :: !EpochNo
  , curEpochRewards :: !(Map (Credential 'Staking c) Coin)
  , curEpochWithdrawals :: !(Map (Credential 'Staking c) Coin)
  }
data RewardsPerEpoch c = RewardsPerEpoch
  { rewardsEpochNo :: !EpochNo
  , rewardsReceivedThisEpoch :: !(Map (Credential 'Staking c) Coin)
  , rewardsWithdrawnThisEpoch :: !(Map (Credential 'Staking c) Coin)
  }

instance Display (RewardsPerEpoch StandardCrypto) where
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

accumNewRewards
  :: Set (Credential 'Staking StandardCrypto)
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> RewardsState StandardCrypto
  -> BlockWithInfo (CardanoBlock StandardCrypto)
  -> ReaderT
      (DbStreamerApp (CardanoBlock StandardCrypto))
      IO
      ( ExtLedgerState (CardanoBlock StandardCrypto)
      , RewardsState StandardCrypto
      , Maybe (RewardsPerEpoch StandardCrypto)
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

replayChain
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
replayChain initLedgerState = do
  runConduit $ foldBlocksWithState GetBlock initLedgerState advanceBlock_

computeRewards
  :: Set (Credential 'Staking StandardCrypto)
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) [RewardsPerEpoch StandardCrypto]
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

replayBenchmarkReport
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) StatsReport
replayBenchmarkReport initLedgerState = do
  report <- runConduit $ void (replayWithBenchmarking initLedgerState) .| calcStatsReport
  report <$ writeReport "Benchmark" report

replayWithBenchmarking
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> ConduitT
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
  logOpts <- logOptionsHandle stdout oVerbose
  withLogFunc (setLogMinLevel oLogLevel $ setLogUseLoc oDebug logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let appConf =
            AppConfig
              { appConfChainDir = oChainDir
              , appConfFilePath = oConfigFilePath
              , appConfReadDiskSnapshot =
                  DiskSnapshot <$> oReadSnapShotSlotNumber <*> pure oSnapShotSuffix
              , appConfWriteDiskSnapshots =
                  DiskSnapshot <$> oWriteSnapShotSlotNumbers <*> pure oSnapShotSuffix
              , appConfStopSlotNumber = oStopSlotNumber
              , appConfValidationMode = oValidationMode
              , appConfWriteBlocksSlotNoSet = Set.fromList oWriteBlocks
              , appConfLogFunc = logFunc
              , appConfRegistry = registry
              }
      void $ runRIO appConf $ runDbStreamerApp $ \initLedger -> do
        app <- ask
        runRIO (app{dsAppOutDir = oOutDir}) $ do
          logInfo $ "Starting to " <> display oCommand
          case oCommand of
            Replay -> void $ replayChain initLedger
            Benchmark -> void $ replayBenchmarkReport initLedger
            Stats -> void $ runConduit $ calcEpochStats initLedger
            ComputeRewards creds ->
              void $ computeRewards (Set.fromList $ NE.toList creds) initLedger

-- -- TxOuts:
-- total <- runRIO (app{dsAppOutDir = mOutDir}) $ countTxOuts initLedger
-- logInfo $ "Total TxOuts: " <> displayShow total
-- runRIO (app{dsAppOutDir = mOutDir}) $ revalidateWriteNewEpochState initLedger
