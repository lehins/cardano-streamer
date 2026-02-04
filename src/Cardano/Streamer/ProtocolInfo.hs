{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Streamer.ProtocolInfo where

import qualified Cardano.Api as Api
import Cardano.Ledger.BaseTypes (SlotNo (..))
import Cardano.Streamer.Common
import Cardano.Streamer.Storage
import Control.Monad.Trans.Except
import Criterion.Measurement (initializeTime)
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (configStorage)
import qualified Ouroboros.Consensus.Node as Node
import qualified Ouroboros.Consensus.Node.InitStorage as Node
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import Ouroboros.Consensus.Storage.ChainDB.Impl.Args (
  cdbImmDbArgs,
  cdbLgrDbArgs,
  completeChainDbArgs,
  updateTracer,
 )
import Ouroboros.Consensus.Storage.LedgerDB.Args (lgrStartSnapshot)
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (
  DiskSnapshot (..),
 )
import Ouroboros.Consensus.Util.CBOR (ReadIncrementalErr)
import RIO.Time

newtype NodeConfigError = NodeConfigError {unNodeConfigError :: Text}
  deriving (Show, Eq)

instance Exception NodeConfigError

readNodeConfig :: MonadIO m => FilePath -> m Api.NodeConfig
readNodeConfig =
  liftIO . throwExceptT . withExceptT NodeConfigError . Api.readNodeConfig . Api.File

readCardanoGenesisConfig :: MonadIO m => Api.NodeConfig -> m Api.GenesisConfig
readCardanoGenesisConfig =
  liftIO . throwExceptT . Api.readCardanoGenesisConfig

readProtocolInfoCardano :: MonadIO m => FilePath -> m (ProtocolInfo (CardanoBlock StandardCrypto))
readProtocolInfoCardano configFilePath = do
  nodeConfig <- readNodeConfig configFilePath
  fst . Api.mkProtocolInfoCardano <$> readCardanoGenesisConfig nodeConfig

-- TODO: Move upstream
instance Exception ReadIncrementalErr

-- | Prepare arguments for chain db.
mkDbArgs ::
  ( MonadIO m
  , MonadReader env m
  , HasResourceRegistry env
  , HasLogFunc env
  ) =>
  FilePath ->
  Maybe DiskSnapshot ->
  ProtocolInfo (CardanoBlock StandardCrypto) ->
  m (ChainDB.ChainDbArgs Identity IO (CardanoBlock StandardCrypto))
mkDbArgs dbDir diskSnapshot ProtocolInfo{pInfoInitLedger, pInfoConfig} = do
  registry <- view registryL
  dbTracer <- mkTracer (Just "Trace") LevelDebug
  let
    ldbArgs = mkLedgerDbArgs InMemV2
    chainDbArgs =
      updateTracer dbTracer $
        completeChainDbArgs
          registry
          pInfoConfig
          pInfoInitLedger
          chunkInfo
          (const True)
          (Node.stdMkChainDbHasFS dbDir)
          (Node.stdMkChainDbHasFS dbDir)
          ldbArgs
          ChainDB.defaultArgs
    -- Overwrite starting disk snapshot for LedgerDB
    lgrDbArgs =
      (cdbLgrDbArgs chainDbArgs)
        { lgrStartSnapshot = diskSnapshot
        }
  logDebug $ "Preparing to open the database: " <> displayShow dbDir
  pure $ chainDbArgs{cdbLgrDbArgs = lgrDbArgs}
  where
    chunkInfo = Node.nodeImmutableDbChunkInfo (configStorage pInfoConfig)

runDbStreamerApp ::
  RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a -> RIO AppConfig a
runDbStreamerApp action = do
  appConf <- ask
  protocolInfo <- readProtocolInfoCardano (appConfFilePath appConf)
  dbArgs <- mkDbArgs (appConfChainDir appConf) (appConfReadDiskSnapshot appConf) protocolInfo
  let
    iDbArgs = cdbImmDbArgs dbArgs
    startSlotNo = SlotNo . dsNumber <$> appConfReadDiskSnapshot appConf
  liftIO initializeTime
  logInfo "withImmutableDb prepare"
  withImmutableDb iDbArgs startSlotNo $ \iDb startPoint -> do
    logInfo "withImmutableDb start"
    startTime <- getCurrentTime
    writeBlocksRef <-
      newIORef (appConfWriteBlocksSlotNoSet appConf, appConfWriteBlocksBlockHashSet appConf)
    ledgerDb <- openLedgerDb (ChainDB.cdbLgrDbArgs dbArgs)
    refSnapshots <- newIORef (appConfWriteDiskSnapshots appConf)
    let app =
          DbStreamerApp
            { dsAppLogFunc = appConfLogFunc appConf
            , dsAppRegistry = appConfRegistry appConf
            , dsAppProtocolInfo = protocolInfo
            , dsAppChainDir = appConfChainDir appConf
            , dsAppChainDbArgs = dbArgs
            , dsAppIDb = iDb
            , dsAppLedgerDb = ledgerDb
            , dsAppOutDir = Nothing
            , dsAppStartPoint = startPoint
            , dsAppStopSlotNo = SlotNo <$> appConfStopSlotNumber appConf
            , dsAppWriteDiskSnapshots = refSnapshots
            , dsAppWriteBlocks = writeBlocksRef
            , dsAppValidationMode = appConfValidationMode appConf
            , dsAppStartTime = startTime
            , dsAppRTSStatsHandle = Nothing
            }
    result <- runRIO app action
    result <$ logInfo "withImmutableDb end"
