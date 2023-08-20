{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.Producer where

import Cardano.Crypto.Hash.Class (hashToStringAsHex)
import Cardano.Ledger.Crypto
import Cardano.Ledger.SafeHash (extractHash)
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState (newEpochStateEpochNo, writeNewEpochState)
import Cardano.Streamer.ProtocolInfo
import Conduit
import Control.Monad.Trans.Except
import Data.Foldable
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
import Ouroboros.Consensus.Util.ResourceRegistry
import RIO.FilePath

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
    sourceBlocksWithState' blockComponent initState () $
      \s a b -> (\(s', c) -> (s', a, c)) <$> action s b

sourceBlocksWithState'
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
sourceBlocksWithState' blockComponent initState acc0 action = do
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

validatePrintBlock
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
validatePrintBlock !prevLedger !block = do
  let (era, slotNo) = getSlotNoWithEra block
  when (unSlotNo slotNo `mod` 10 == 0) $
    logSticky $
      displayShow era <> ":[" <> displayShow slotNo <> "]"
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  let result =
        runExcept $
          tickThenApplyLedgerResult ledgerCfg block prevLedger
  case result of
    Right lr -> pure $ lrResult lr
    Left err -> do
      logSticky $
        displayShow era <> ":[" <> displayShow slotNo <> "]"
      let rawBlock = getRawBlock block
          blockHashHex = hashToStringAsHex (extractHash (rawBlockHash rawBlock))
      logError "Encountered an error while validating a block: "
      mOutDir <- dsAppOutDir <$> ask
      forM_ mOutDir $ \outDir -> do
        let prefix = outDir </> show (unSlotNo slotNo) <> "_" <> blockHashHex
            mkTxFileName ix = prefix <> "#" <> show ix <.> "cbor"
            fileNameBlock = prefix <.> "cbor"
        writeFileBinary fileNameBlock (rawBlockBytes rawBlock)
        writeNewEpochState (outDir </> show (unSlotNo slotNo) <.> "cbor") prevLedger
        applyBlockTxs
          (liftIO . print)
          (zipWithM_ (\ix -> writeTx (mkTxFileName ix)) [0 :: Int ..])
          block
      throwString . show $ err

validateLedger
  :: LedgerSupportsProtocol b
  => ExtLedgerState b
  -> RIO (DbStreamerApp b) (ExtLedgerState b)
validateLedger initLedgerState =
  runConduit $ foldBlocksWithState GetBlock initLedgerState validateBlock

validatePrintLedger
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
validatePrintLedger initLedgerState =
  runConduit $ foldBlocksWithState GetBlock initLedgerState validatePrintBlock

revalidatePrintBlock
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), BlockPrecis)
revalidatePrintBlock !prevLedger !block = do
  let !blockPrecis = getBlockPrecis block
      EpochNo epochNo = newEpochStateEpochNo prevLedger
  when (unSlotNo (bpSlotNo blockPrecis) `mod` 10 == 0) $
    logSticky $
      "["
        <> displayShow (bpEra blockPrecis)
        <> " <epoch "
        <> displayShow epochNo
        <> ">: "
        <> displayShow (bpSlotNo blockPrecis)
        <> "]"
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  let !result = lrResult $ tickThenReapplyLedgerResult ledgerCfg block prevLedger
  pure (result, blockPrecis)

advanceBlockGranular
  :: ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> SlotNo
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> ExtLedgerState (CardanoBlock StandardCrypto)
       -> CardanoBlock StandardCrypto
       -> a
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) b
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), a, b)
advanceBlockGranular inspectTickState inspectBlockState !prevLedger !block = do
  let (era, slotNo) = getSlotNoWithEra block
  when (unSlotNo slotNo `mod` 10 == 0) $
    logSticky $
      "["
        <> displayShow era
        <> ": "
        <> displayShow slotNo
        <> "]"
  app <- ask
  let ledgerCfg = ExtLedgerCfg . pInfoConfig $ dsAppProtocolInfo app
      lrTick = applyChainTickLedgerResult ledgerCfg slotNo prevLedger
  a <- inspectTickState (lrResult lrTick) slotNo
  case dsValidationMode app of
    FullValidation -> do undefined
    ReValidation -> do
      let lrBlock = reapplyBlockLedgerResult ledgerCfg block (lrResult lrTick)
      b <- inspectBlockState (lrResult lrTick) (lrResult lrBlock) block a
      pure (lrResult lrBlock, a, b)

advanceBlock
  :: ( Ticked (ExtLedgerState (CardanoBlock StandardCrypto))
       -> ExtLedgerState (CardanoBlock StandardCrypto)
       -> CardanoBlock StandardCrypto
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO
      (DbStreamerApp (CardanoBlock StandardCrypto))
      (ExtLedgerState (CardanoBlock StandardCrypto), a)
advanceBlock inspectBlockState !prevLedger !block = do
  (ns, _, b) <-
    advanceBlockGranular
      (\_ _ -> pure ())
      (\ts s blk _ -> inspectBlockState ts s blk)
      prevLedger
      block
  pure (ns, b)

-- data RewardsState = RewardsState
--   { curEpoch :: !EpochNo
--   , curEpochRewards :: !(Map (Credential 'Staking c) Coin)
--   , curEpochWithdrawals :: !(Map (Credential 'Staking c) Coin)
--   , rewardsPerEpoch :: !(Map EpochNo (Map (Credential 'Staking c) Coin))
--   , withdrawalsPerEpoch :: !(Map EpochNo (Map (Credential 'Staking c) Coin))
--   }

-- accumNewRewards creds rs prevExtLedgerState block = do
--   (newExtLedgerState, _) <- revalidatePrintBlock prevExtLedgerState block
--   let newEpochWithdrawals = filterBlockWithdrawals creds block
--         Map.unionWith (<>) () (curEpochWithdrawals rs)
--   case detectNewRewards creds (curEpoch rs) (curEpochRewards rs) (curEpochWithdrawals rs) newExtLedgerState of
--     Nothing -> (prevEpochNo, newExtLedgerState, rewards)

countTxOuts
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) Int
countTxOuts initLedgerState =
  getSum
    <$> runConduit
      ( void (sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock)
          .| foldMapMC (liftIO . evaluate . foldMap' (fromIntegral . tpOutsCount) . bpTxsPrecis)
      )

revalidateWriteNewEpochState
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) ()
revalidateWriteNewEpochState initLedgerState = do
  (extLedgerState, mBlockPrecis) <-
    runConduit $
      sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock `fuseBoth` lastC
  case mBlockPrecis of
    Nothing -> logError "No blocks where discovered on chain"
    Just lastBlockPrecis -> do
      mDir <- dsAppOutDir <$> ask
      forM_ mDir $ \dir -> do
        let slotNo = unSlotNo (bpSlotNo lastBlockPrecis)
            filePath = dir </> "new-epoch-state_" ++ show slotNo ++ ".cbor"
        writeNewEpochState filePath extLedgerState

-- runApp
--   :: FilePath
--   -> FilePath
--   -> Maybe FilePath
--   -> Maybe DiskSnapshot
--   -> Bool
--   -> IO ()
-- runApp dbDir confFilePath mOutDir diskSnapshot verbose = do
runApp :: Opts -> IO ()
runApp Opts{..} = do
  logOpts <- logOptionsHandle stdout oVerbose
  withLogFunc (setLogMinLevel oLogLevel $ setLogUseLoc oDebug logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let appConf =
            AppConfig
              { appConfDbDir = oChainDir
              , appConfFilePath = oConfigFilePath
              , appConfDiskSnapshot = oDiskSnapShot
              , appConfValidationMode = oValidationMode
              , appConfLogFunc = logFunc
              , appConfRegistry = registry
              }
      void $ runRIO appConf $ runDbStreamerApp $ \initLedger -> do
        app <- ask
        runRIO (app{dsAppOutDir = oOutDir}) $ validatePrintLedger initLedger

-- -- TxOuts:
-- total <- runRIO (app{dsAppOutDir = mOutDir}) $ countTxOuts initLedger
-- logInfo $ "Total TxOuts: " <> displayShow total
-- runRIO (app{dsAppOutDir = mOutDir}) $ revalidateWriteNewEpochState initLedger
