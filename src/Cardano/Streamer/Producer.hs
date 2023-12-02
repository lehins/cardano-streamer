{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.Producer where

import Cardano.Crypto.Hash.Class (hashToTextAsHex)
import Cardano.Ledger.Coin
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.SafeHash (extractHash)
import Cardano.Streamer.Benchmark
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState (detectNewRewards, extLedgerStateEpochNo, writeNewEpochState)
import Cardano.Streamer.ProtocolInfo
import Conduit
import Control.Monad.Trans.Except
import Data.Foldable
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Merge.Strict as Map
import qualified Data.Map.Strict as Map
import Data.Monoid
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HeaderValidation (
  AnnTip,
  HasAnnTip (..),
  annTipPoint,
  headerStateTip,
 )
import Ouroboros.Consensus.Ledger.Abstract (
  applyBlockLedgerResult,
  reapplyBlockLedgerResult,
  tickThenApplyLedgerResult,
  tickThenReapplyLedgerResult,
 )
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (lrResult), applyChainTickLedgerResult)
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState), Ticked)
import Ouroboros.Consensus.Ledger.SupportsProtocol (LedgerSupportsProtocol)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import Ouroboros.Consensus.Util.ResourceRegistry
import RIO.FilePath
import qualified RIO.Set as Set
import qualified RIO.Text as T

sourceBlocks
  :: ( MonadIO m
     , MonadReader env m
     , HasImmutableDb env blk
     , HasResourceRegistry env
     , HasHeader blk
     , HasAnnTip blk
     )
  => BlockComponent blk b
  -> WithOrigin (AnnTip blk)
  -> ConduitT i b m ()
sourceBlocks blockComponent withOriginAnnTip = do
  registry <- view registryL
  iDb <- view iDbL
  itr <- liftIO $ case withOriginAnnTip of
    Origin ->
      ImmutableDB.streamAll iDb registry blockComponent
    NotOrigin annTip ->
      ImmutableDB.streamAfterKnownPoint iDb registry blockComponent (annTipPoint annTip)
  fix $ \loop ->
    liftIO (ImmutableDB.iteratorNext itr) >>= \case
      ImmutableDB.IteratorExhausted -> pure ()
      ImmutableDB.IteratorResult b -> yield b >> loop

foldBlocksWithState
  :: ( MonadIO m
     , MonadReader env m
     , HasImmutableDb env blk
     , HasResourceRegistry env
     , HasHeader blk
     , HasAnnTip blk
     )
  => BlockComponent blk b
  -> ExtLedgerState blk
  -- ^ Initial ledger state
  -> (ExtLedgerState blk -> b -> m (ExtLedgerState blk))
  -- ^ Function to process each block with current ledger state
  -> ConduitT a c m (ExtLedgerState blk)
foldBlocksWithState blockComponent initState action = do
  let withOriginAnnTip = headerStateTip (headerState initState)
  sourceBlocks blockComponent withOriginAnnTip .| foldMC action initState

sourceBlocksWithState
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasImmutableDb env blk
     , HasHeader blk
     , HasAnnTip blk
     )
  => BlockComponent blk b
  -> ExtLedgerState blk
  -> (ExtLedgerState blk -> b -> m (ExtLedgerState blk, c))
  -> ConduitT a c m (ExtLedgerState blk)
sourceBlocksWithState blockComponent initState action =
  fmap fst $
    sourceBlocksWithAccState blockComponent initState () $
      \s a b -> (\(s', c) -> (s', a, c)) <$> action s b

sourceBlocksWithAccState
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasImmutableDb env blk
     , HasHeader blk
     , HasAnnTip blk
     )
  => BlockComponent blk b
  -> ExtLedgerState blk
  -> e
  -> (ExtLedgerState blk -> e -> b -> m (ExtLedgerState blk, e, c))
  -> ConduitT a c m (ExtLedgerState blk, e)
sourceBlocksWithAccState blockComponent initState acc0 action = do
  sourceBlocks blockComponent withOriginAnnTip .| go initState acc0
  where
    withOriginAnnTip = headerStateTip (headerState initState)
    go !ledgerState !acc =
      await >>= \case
        Nothing -> pure (ledgerState, acc)
        Just b -> do
          (ledgerState', acc', c) <- lift $ action ledgerState acc b
          yield c
          go ledgerState' acc'

validateBlock
  :: LedgerSupportsProtocol blk
  => ExtLedgerState blk
  -> blk
  -> RIO (DbStreamerApp blk) (ExtLedgerState blk)
validateBlock !prevLedger !block = do
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  either (throwString . show) (pure . lrResult) $
    runExcept $
      tickThenApplyLedgerResult ledgerCfg block prevLedger

-- validatePrintBlock
--   :: ExtLedgerState (CardanoBlock StandardCrypto)
--   -> CardanoBlock StandardCrypto
--   -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
-- validatePrintBlock !prevLedger !block = do
--   let (era, slotNo) = getSlotNoWithEra block
--   when (unSlotNo slotNo `mod` 10 == 0) $
--     logSticky $
--       displayShow era <> ":[" <> displayShow slotNo <> "]"
--   ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
--   let result =
--         runExcept $
--           tickThenApplyLedgerResult ledgerCfg block prevLedger
--   case result of
--     Right lr -> pure $ lrResult lr
--     Left err -> do
--       logSticky $
--         displayShow era <> ":[" <> displayShow slotNo <> "]"
--       let rawBlock = getRawBlock block
--           blockHashHex = hashToTextAsHex (extractHash (rawBlockHash rawBlock))
--       logError "Encountered an error while validating a block: "
--       mOutDir <- dsAppOutDir <$> ask
--       forM_ mOutDir $ \outDir -> do
--         let prefix = outDir </> show (unSlotNo slotNo) <> "_" <> blockHashHex
--             mkTxFileName ix = prefix <> "#" <> show ix <.> "cbor"
--             fileNameBlock = prefix <.> "cbor"
--         writeFileBinary fileNameBlock (rawBlockBytes rawBlock)
--         writeNewEpochState (outDir </> show (unSlotNo slotNo) <.> "cbor") prevLedger
--         applyBlockTxs
--           (liftIO . print)
--           (zipWithM_ (\ix -> writeTx (mkTxFileName ix)) [0 :: Int ..])
--           block
--       throwString . show $ err

validateLedger
  :: LedgerSupportsProtocol b
  => ExtLedgerState b
  -> RIO (DbStreamerApp b) (ExtLedgerState b)
validateLedger initLedgerState =
  runConduit $ foldBlocksWithState GetBlock initLedgerState validateBlock

revalidatePrintBlock
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), BlockSummary)
revalidatePrintBlock !prevLedger !block = do
  let !blockSummary = getBlockSummary block
      EpochNo epochNo = extLedgerStateEpochNo prevLedger
  when (unSlotNo (bpSlotNo blockSummary) `mod` 100 == 0) $
    logSticky $
      "["
        <> displayShow (bpEra blockSummary)
        <> " <epoch "
        <> displayShow epochNo
        <> ">: "
        <> displayShow (bpSlotNo blockSummary)
        <> "]"
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  let !result = lrResult $ tickThenReapplyLedgerResult ledgerCfg block prevLedger
  pure (result, blockSummary)

advanceBlockGranular
  :: ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> SlotNo
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> ExtLedgerState (CardanoBlock StandardCrypto)
       -> a
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) b
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), b)
advanceBlockGranular inspectTickState inspectBlockState !prevLedger !block = do
  let (era, slotNo) = getSlotNoWithEra block
  when (unSlotNo slotNo `mod` 100 == 0) $ do
    let epochNo = extLedgerStateEpochNo prevLedger
    logSticky $
      "["
        <> displayShow era
        <> ": EpochNo "
        <> display (unEpochNo epochNo)
        <> " - "
        <> displayShow slotNo
        <> "]"
  app <- ask
  let ledgerCfg = ExtLedgerCfg . pInfoConfig $ dsAppProtocolInfo app
      lrTick = applyChainTickLedgerResult ledgerCfg slotNo prevLedger
      lrTickResult = lrResult lrTick
  a <- inspectTickState lrTickResult slotNo
  case dsValidationMode app of
    FullValidation -> do
      case runExcept (applyBlockLedgerResult ledgerCfg block lrTickResult) of
        Right lrBlock -> do
          let lrBlockResult = lrResult lrBlock
          b <- inspectBlockState lrTickResult lrBlockResult a
          pure (lrBlockResult, b)
        Left errorMessage -> reportValidationError errorMessage era slotNo block prevLedger
    ReValidation -> do
      let lrBlock = reapplyBlockLedgerResult ledgerCfg block (lrResult lrTick)
      b <- inspectBlockState (lrResult lrTick) (lrResult lrBlock) a
      pure (lrResult lrBlock, b)

reportValidationError
  :: ( MonadReader (DbStreamerApp blk) m
     , MonadIO m
     , Show b
     , Show a
     , Crypto c
     )
  => a
  -> b
  -> SlotNo
  -> CardanoBlock c
  -> ExtLedgerState (CardanoBlock c)
  -> m d
reportValidationError errorMessage era slotNo block ledgerState = do
  logSticky $
    displayShow era <> ":[" <> displayShow slotNo <> "]"
  let rawBlock = getRawBlock block
      blockHashHex = hashToTextAsHex (extractHash (rawBlockHash rawBlock))
  logError $
    "Encountered an error while validating a block: " <> display blockHashHex
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
  throwString $ show errorMessage

advanceBlock
  :: ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> ExtLedgerState (CardanoBlock StandardCrypto)
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), a)
advanceBlock inspectBlockState !prevLedger !block =
  advanceBlockGranular
    (\_ _ -> pure ())
    (\ts s _ -> inspectBlockState ts s)
    prevLedger
    block

advanceBlock_
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto))
advanceBlock_ !prevLedger !block =
  fst <$> advanceBlockGranular (\_ _ -> pure ()) (\_ _ _ -> pure ()) prevLedger block

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
  -> CardanoBlock StandardCrypto
  -> ReaderT
      (DbStreamerApp (CardanoBlock StandardCrypto))
      IO
      ( ExtLedgerState (CardanoBlock StandardCrypto)
      , RewardsState StandardCrypto
      , Maybe (RewardsPerEpoch StandardCrypto)
      )
accumNewRewards creds prevExtLedgerState rs block = do
  let calcRewards tels _els =
        detectNewRewards
          creds
          (curEpoch rs)
          (curEpochRewards rs)
          (curEpochWithdrawals rs)
          tels
  (newExtLedgerState, (newEpochNo, mRewards)) <- advanceBlock calcRewards prevExtLedgerState block
  let curBlockWithdrawals = filterBlockWithdrawals creds block
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

countTxOuts
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) Int
countTxOuts initLedgerState =
  getSum
    <$> runConduit
      ( void (sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock)
          .| foldMapMC (liftIO . evaluate . foldMap' (fromIntegral . tpOutsCount) . bpTxsSummary)
      )

revalidateWriteNewEpochState
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) ()
revalidateWriteNewEpochState initLedgerState = do
  (extLedgerState, mBlockSummary) <-
    runConduit $
      sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock `fuseBoth` lastC
  case mBlockSummary of
    Nothing -> logError "No blocks where discovered on chain"
    Just lastBlockSummary -> do
      mDir <- dsAppOutDir <$> ask
      forM_ mDir $ \dir -> do
        let slotNo = unSlotNo (bpSlotNo lastBlockSummary)
            filePath = dir </> "new-epoch-state_" ++ show slotNo ++ ".cbor"
        writeNewEpochState filePath extLedgerState

replayChain
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
replayChain initLedgerState =
  runConduit $ foldBlocksWithState GetBlock initLedgerState advanceBlock_

computeRewards
  :: Set (Credential 'Staking StandardCrypto)
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) [RewardsPerEpoch StandardCrypto]
computeRewards creds initLedgerState = do
  -- (extLedgerState, rs) <-
  runConduit $
    void (sourceBlocksWithAccState GetBlock initLedgerState emptyRewardsState (accumNewRewards creds))
      .| concatMapC id
      .| sinkList
  where
    --   `fuseBoth` lastC
    -- pure $ fromMaybe [] rs

    emptyRewardsState = RewardsState 0 mempty mempty

replayCalcStatsReport
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) StatsReport
replayCalcStatsReport initLedgerState =
  runConduit $ void (replayWithBenchmarking initLedgerState) .| calcStatsReport

replayWithBenchmarking
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> ConduitT
      a
      Stat
      (RIO (DbStreamerApp (CardanoBlock StandardCrypto)))
      (ExtLedgerState (CardanoBlock StandardCrypto))
replayWithBenchmarking initLedgerState =
  withBenchmarking $ \benchRunTick benchRunBlock ->
    sourceBlocksWithState GetBlock initLedgerState (advanceBlockGranular benchRunTick benchRunBlock)

runApp :: Opts -> IO ()
runApp Opts{..} = do
  logOpts <- logOptionsHandle stdout oVerbose
  withLogFunc (setLogMinLevel oLogLevel $ setLogUseLoc oDebug logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let appConf =
            AppConfig
              { appConfDbDir = oChainDir
              , appConfFilePath = oConfigFilePath
              , appConfReadDiskSnapshot =
                  DiskSnapshot <$> oReadSnapShotSlotNumber <*> pure oSnapShotSuffix
              , appConfWriteDiskSnapshots =
                  DiskSnapshot <$> oWriteSnapShotSlotNumbers <*> pure oSnapShotSuffix
              , appConfStopSlotNumber = oStopSlotNumber
              , appConfValidationMode = oValidationMode
              , appConfLogFunc = logFunc
              , appConfRegistry = registry
              }
      void $ runRIO appConf $ runDbStreamerApp $ \initLedger -> do
        app <- ask
        runRIO (app{dsAppOutDir = oOutDir}) $
          case oCommand of
            Replay -> void $ replayChain initLedger
            Benchmark -> do
              report <- replayCalcStatsReport initLedger
              logInfo $ display report
            ComputeRewards creds ->
              void $ computeRewards (Set.fromList $ NE.toList creds) initLedger

-- -- TxOuts:
-- total <- runRIO (app{dsAppOutDir = mOutDir}) $ countTxOuts initLedger
-- logInfo $ "Total TxOuts: " <> displayShow total
-- runRIO (app{dsAppOutDir = mOutDir}) $ revalidateWriteNewEpochState initLedger
