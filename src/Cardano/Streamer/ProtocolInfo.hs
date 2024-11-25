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
import Cardano.Streamer.Benchmark
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState
import Codec.Serialise (Serialise (decode))
import Control.Monad.Trans.Except
import Ouroboros.Consensus.Block (BlockProtocol, ConvertRawHash, GetPrevHash)
import Ouroboros.Consensus.Block.NestedContent (NestedCtxt)
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (configCodec, configStorage)
import qualified Ouroboros.Consensus.Fragment.InFuture as InFuture
import Ouroboros.Consensus.HeaderValidation (AnnTip)
import Ouroboros.Consensus.Ledger.Extended (
  ExtLedgerState,
  decodeExtLedgerState,
  encodeExtLedgerState,
 )
import qualified Ouroboros.Consensus.Node as Node
import qualified Ouroboros.Consensus.Node.InitStorage as Node
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Protocol.Abstract (ChainDepState)
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import Ouroboros.Consensus.Storage.ChainDB.Impl.Args (
  cdbImmDbArgs,
  completeChainDbArgs,
  updateTracer,
 )
import Ouroboros.Consensus.Storage.ChainDB.Impl.LgrDB (lgrHasFS)
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.ImmutableDB.Impl (ImmutableDbArgs)
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (
  DiskSnapshot (..),
  readSnapshot,
  writeSnapshot,
 )
import Ouroboros.Consensus.Storage.Serialisation (
  DecodeDisk (decodeDisk),
  DecodeDiskDep,
  EncodeDisk (encodeDisk),
  HasBinaryBlockInfo,
  ReconstructNestedCtxt,
 )
import Ouroboros.Consensus.Util.CBOR (ReadIncrementalErr)
import Ouroboros.Consensus.Util.ResourceRegistry (runWithTempRegistry)
import Ouroboros.Network.Block (HeaderHash)
import RIO.Directory (doesFileExist, removeFile)
import qualified RIO.Text as T
import RIO.Time

newtype NodeConfigError = NodeConfigError {unNodeConfigError :: Text}
  deriving (Show, Eq)

instance Exception NodeConfigError

readNodeConfig :: MonadIO m => FilePath -> m Api.NodeConfig
readNodeConfig =
  liftIO . throwExceptT . withExceptT NodeConfigError . Api.readNodeConfig . Api.File

readCardanoGenesisConfig :: MonadIO m => Api.NodeConfig -> m Api.GenesisConfig
readCardanoGenesisConfig =
  liftIO . throwExceptT . Api.readCardanoGenesisConfig Nothing

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
  , Show (Header blk)
  , Node.RunNode blk
  ) =>
  FilePath ->
  ProtocolInfo blk ->
  m (ChainDB.ChainDbArgs Identity IO blk)
mkDbArgs dbDir ProtocolInfo{pInfoInitLedger, pInfoConfig} = do
  registry <- view registryL
  dbTracer <- mkTracer (Just "Trace") LevelDebug
  let
    chainDbArgs =
      updateTracer dbTracer $
        completeChainDbArgs
          registry
          InFuture.dontCheck
          pInfoConfig
          pInfoInitLedger
          chunkInfo
          (const True)
          (Node.stdMkChainDbHasFS dbDir)
          (Node.stdMkChainDbHasFS dbDir)
          ChainDB.defaultArgs
  logDebug $ "Preparing to open the database: " <> displayShow dbDir
  pure chainDbArgs
  where
    chunkInfo = Node.nodeImmutableDbChunkInfo (configStorage pInfoConfig)

withImmutableDb ::
  ( MonadUnliftIO m
  , MonadReader env m
  , GetPrevHash blk
  , ConvertRawHash blk
  , EncodeDisk blk blk
  , DecodeDisk blk (LByteString -> blk)
  , DecodeDiskDep (NestedCtxt Header) blk
  , ReconstructNestedCtxt Header blk
  , HasBinaryBlockInfo blk
  , HasLogFunc env
  ) =>
  ImmutableDbArgs Identity IO blk ->
  (ImmutableDB.ImmutableDB IO blk -> m b) ->
  m b
withImmutableDb iDbArgs action = do
  res <- withRunInIO $ \run ->
    ImmutableDB.withDB (ImmutableDB.openDB iDbArgs runWithTempRegistry) $ \db ->
      run $ do
        logDebug "Opened an immutable database"
        action db
  logDebug "Closed an immutable database"
  pure res

writeLedgerState ::
  ( MonadIO m
  , MonadReader (DbStreamerApp (CardanoBlock StandardCrypto)) m
  -- , EncodeDisk (CardanoBlock StandardCrypto) (LedgerState (CardanoBlock StandardCrypto))
  -- , EncodeDisk (CardanoBlock StandardCrypto) (ChainDepState (BlockProtocol (CardanoBlock StandardCrypto)))
  -- , EncodeDisk (CardanoBlock StandardCrypto) (AnnTip (CardanoBlock StandardCrypto))
  ) =>
  DiskSnapshot ->
  ExtLedgerState (CardanoBlock StandardCrypto) ->
  m ()
writeLedgerState diskSnapshot extLedgerState = do
  ledgerDbFS <- lgrHasFS . ChainDB.cdbLgrDbArgs . dsAppChainDbArgs <$> ask
  ccfg <- configCodec . pInfoConfig . dsAppProtocolInfo <$> ask
  let
    extLedgerStateEncoder =
      encodeExtLedgerState (encodeDisk ccfg) (encodeDisk ccfg) (encodeDisk ccfg)
  snapshotFilePath <- getDiskSnapshotFilePath diskSnapshot
  whenM (doesFileExist snapshotFilePath) $ do
    -- TODO: add interactive mode and ask for confirmation
    removeFile snapshotFilePath
    logWarn $
      "DiskSnapshot at path already exists: "
        <> display (T.pack snapshotFilePath)
        <> ". Removed, so it can be overwritten!"
  measure <-
    measureAction_ $ liftIO $ writeSnapshot ledgerDbFS extLedgerStateEncoder diskSnapshot extLedgerState
  logInfo $
    "Written DiskSnapshot to: " <> display (T.pack snapshotFilePath) <> " in " <> display measure
  when True $ do
    -- TODO: Add cli option
    let nesFilePath = snapshotFilePath <> "_nes.cbor"
    measureNes <- measureAction_ $ writeNewEpochState nesFilePath extLedgerState
    logInfo $
      "Written NewEpochState to: " <> display (T.pack nesFilePath) <> " in " <> display measureNes

readInitLedgerState ::
  ( DecodeDisk blk (LedgerState blk)
  , DecodeDisk blk (ChainDepState (BlockProtocol blk))
  , DecodeDisk blk (AnnTip blk)
  , Serialise (HeaderHash blk)
  ) =>
  DiskSnapshot ->
  RIO (DbStreamerApp blk) (ExtLedgerState blk)
readInitLedgerState diskSnapshot = do
  snapshotFilePath <- getDiskSnapshotFilePath diskSnapshot
  logInfo $ "Reading initial ledger state: " <> display (T.pack snapshotFilePath)
  ledgerDbFS <- lgrHasFS . ChainDB.cdbLgrDbArgs . dsAppChainDbArgs <$> ask
  ccfg <- configCodec . pInfoConfig . dsAppProtocolInfo <$> ask
  let
    extLedgerStateDecoder =
      decodeExtLedgerState (decodeDisk ccfg) (decodeDisk ccfg) (decodeDisk ccfg)
  (ledgerState, measure) <-
    measureAction $
      liftIO $
        throwExceptT $
          readSnapshot ledgerDbFS extLedgerStateDecoder decode diskSnapshot
  logInfo $ "Done reading the ledger state in: " <> display measure
  pure ledgerState

getInitLedgerState ::
  ( DecodeDisk blk (LedgerState blk)
  , DecodeDisk blk (ChainDepState (BlockProtocol blk))
  , DecodeDisk blk (AnnTip blk)
  , Serialise (HeaderHash blk)
  ) =>
  Maybe DiskSnapshot ->
  RIO (DbStreamerApp blk) (ExtLedgerState blk)
getInitLedgerState = \case
  Nothing -> pInfoInitLedger . dsAppProtocolInfo <$> ask
  Just diskSnapshot -> readInitLedgerState diskSnapshot

runDbStreamerApp ::
  ( ExtLedgerState (CardanoBlock StandardCrypto) ->
    RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
  ) ->
  RIO AppConfig a
runDbStreamerApp action = do
  appConf <- ask
  protocolInfo <- readProtocolInfoCardano (appConfFilePath appConf)
  dbArgs <- mkDbArgs (appConfChainDir appConf) protocolInfo
  let iDbArgs = cdbImmDbArgs dbArgs
  logInfo "withImmutableDb prepare"
  withImmutableDb iDbArgs $ \iDb -> do
    logInfo "withImmutableDb start"
    startTime <- getCurrentTime
    writeBlocksRef <-
      newIORef (appConfWriteBlocksSlotNoSet appConf, appConfWriteBlocksBlockHashSet appConf)
    let app =
          DbStreamerApp
            { dsAppLogFunc = appConfLogFunc appConf
            , dsAppRegistry = appConfRegistry appConf
            , dsAppProtocolInfo = protocolInfo
            , dsAppChainDir = appConfChainDir appConf
            , dsAppChainDbArgs = dbArgs
            , dsAppIDb = iDb
            , dsAppOutDir = Nothing
            , dsAppStopSlotNo = SlotNo <$> appConfStopSlotNumber appConf
            , dsAppWriteDiskSnapshots = appConfWriteDiskSnapshots appConf
            , dsAppWriteBlocks = writeBlocksRef
            , dsAppValidationMode = appConfValidationMode appConf
            , dsAppStartTime = startTime
            }
    res <- runRIO app (getInitLedgerState (appConfReadDiskSnapshot appConf) >>= action)
    logInfo "withImmutableDb end"
    pure res
