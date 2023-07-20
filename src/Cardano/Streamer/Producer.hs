{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.Streamer.Producer where

import Cardano.Crypto.Hash.Class (hashToStringAsHex)
import Cardano.Ledger.Binary.Plain (serialize)
import Cardano.Ledger.Crypto
import Cardano.Ledger.SafeHash (extractHash)
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState (encodeNewEpochState)
import Cardano.Streamer.ProtocolInfo
import Conduit
import Control.Monad.Trans.Except
import Data.ByteString.Lazy as BSL
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
import Ouroboros.Consensus.Ledger.Abstract (tickThenApplyLedgerResult, tickThenReapplyLedgerResult)
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (lrResult))
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState))
import Ouroboros.Consensus.Ledger.SupportsProtocol (LedgerSupportsProtocol)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
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
sourceBlocksWithState blockComponent initState action = do
  sourceBlocks blockComponent withOriginAnnTip .| go initState
  where
    withOriginAnnTip = headerStateTip (headerState initState)
    go ledgerState =
      await >>= \case
        Nothing -> pure ledgerState
        Just b -> do
          (ledgerState', c) <- lift $ action ledgerState b
          yield c
          go ledgerState'

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
  let slotNo = getSlotNo block
  when (unSlotNo slotNo `mod` 10 == 0) $
    logSticky $
      "[" <> displayShow slotNo <> "]"
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  let result =
        runExcept $
          tickThenApplyLedgerResult ledgerCfg block prevLedger
  case result of
    Right lr -> pure $ lrResult lr
    Left err -> do
      let rawBlock = getRawBlock block
          blockHashHex = hashToStringAsHex (extractHash (rawBlockHash rawBlock))
      logError "Encountered an error while validating a block: "
      mOutDir <- dsAppOutDir <$> ask
      forM_ mOutDir $ \outDir ->
        let fileName = show (unSlotNo slotNo) <> "_" <> blockHashHex <.> "cbor"
         in writeFileBinary (outDir </> fileName) (rawBlockBytes rawBlock)
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
  when (unSlotNo (bpSlotNo blockPrecis) `mod` 10 == 0) $
    logSticky $
      "[" <> displayShow (bpEra blockPrecis) <> ": " <> displayShow (bpSlotNo blockPrecis) <> "]"
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  let !result = lrResult $ tickThenReapplyLedgerResult ledgerCfg block prevLedger
  pure (result, blockPrecis)

countTxOuts
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) Int
countTxOuts initLedgerState =
  getSum
    <$> runConduit
      ( void (sourceBlocksWithState GetBlock initLedgerState revalidatePrintBlock)
          .| foldMapMC (liftIO . evaluate . foldMap' (fromIntegral . tpOutsCount) . bpTxsPrecis)
      )

writeNewEpochState :: (Crypto c, MonadIO m) => FilePath -> ExtLedgerState (CardanoBlock c) -> m ()
writeNewEpochState filePath = liftIO . BSL.writeFile filePath . serialize . encodeNewEpochState


revalidateWriteNewEpochState ::
     ExtLedgerState (CardanoBlock StandardCrypto)
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

runApp
  :: FilePath
  -- ^ Db directory
  -> FilePath
  -- ^ Config file path
  -> Maybe FilePath
  -- ^ Directory where requested files should be written to.
  -> Maybe DiskSnapshot
  -- ^ Where to start from
  -> Bool
  -- ^ Verbose logging?
  -> IO ()
runApp dbDir confFilePath mOutDir diskSnapshot verbose = do
  logOpts <- logOptionsHandle stdout verbose
  withLogFunc (setLogUseLoc False logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let appConf =
            AppConfig
              { appConfDbDir = dbDir
              , appConfFilePath = confFilePath
              , appConfDiskSnapshot = diskSnapshot
              , appConfLogFunc = logFunc
              , appConfRegistry = registry
              }
      void $ runRIO appConf $ runDbStreamerApp $ \initLedger -> do
        app <- ask
        -- -- Validate:
        runRIO (app{dsAppOutDir = mOutDir}) $ validatePrintLedger initLedger
        -- -- TxOuts:
        -- total <- runRIO (app{dsAppOutDir = mOutDir}) $ countTxOuts initLedger
        -- logInfo $ "Total TxOuts: " <> displayShow total
        -- runRIO (app{dsAppOutDir = mOutDir}) $ revalidateWriteNewEpochState initLedger
