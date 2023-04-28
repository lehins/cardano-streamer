{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.Streamer.Producer where

import Cardano.Ledger.Crypto
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.ProtocolInfo
import Conduit
import Control.Monad.Trans.Except
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HeaderValidation (
  AnnTip,
  HasAnnTip (..),
  annTipPoint,
  headerStateTip,
 )
import Ouroboros.Consensus.Ledger.Abstract (tickThenApplyLedgerResult)
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (lrResult))
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState))
import Ouroboros.Consensus.Ledger.SupportsProtocol (LedgerSupportsProtocol)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import Ouroboros.Consensus.Util.ResourceRegistry

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
validateBlock prevLedger block = do
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  either (throwString . show) (pure . lrResult) $
    runExcept $
      tickThenApplyLedgerResult ledgerCfg block prevLedger

validatePrintBlock
  :: ExtLedgerState (CardanoBlock StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) (ExtLedgerState (CardanoBlock StandardCrypto))
validatePrintBlock prevLedger block = do
  logSticky $
    "["
      <> displayShow (getSlotNo block)
      <> "]"
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  either (throwString . show) (pure . lrResult) $
    runExcept $
      tickThenApplyLedgerResult ledgerCfg block prevLedger

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

runApp
  :: FilePath
  -- ^ Db directory
  -> FilePath
  -- ^ Config file path
  -> Maybe DiskSnapshot
  -- ^ Where to start from
  -> Bool
  -- ^ Verbose logging?
  -> IO ()
runApp dbDir confFilePath diskSnapshot verbose = do
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
      void $ runRIO appConf $ runDbStreamerApp validatePrintLedger
