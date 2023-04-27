{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Streamer.ProtocolInfo where

import qualified Cardano.Api as Api
import Cardano.Streamer.Common
import Codec.Serialise (Serialise (decode))
import Control.Monad.Trans.Except
import Ouroboros.Consensus.Block (BlockProtocol, ConvertRawHash, GetPrevHash)
import Ouroboros.Consensus.Block.NestedContent (NestedCtxt)
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (configCodec, configSecurityParam, configStorage)
import qualified Ouroboros.Consensus.Fragment.InFuture as InFuture
import Ouroboros.Consensus.HeaderValidation (AnnTip)
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerState, decodeExtLedgerState)
import qualified Ouroboros.Consensus.Node as Node
import qualified Ouroboros.Consensus.Node.InitStorage as Node
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Protocol.Abstract (ChainDepState)
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import Ouroboros.Consensus.Storage.ChainDB.Impl.Args (fromChainDbArgs)
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.ImmutableDB.Impl (ImmutableDbArgs)
import Ouroboros.Consensus.Storage.LedgerDB.DiskPolicy (
  SnapshotInterval (DefaultSnapshotInterval),
  defaultDiskPolicy,
 )
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..), readSnapshot)
import Ouroboros.Consensus.Storage.Serialisation (
  DecodeDisk (decodeDisk),
  DecodeDiskDep,
  EncodeDisk,
  HasBinaryBlockInfo,
  ReconstructNestedCtxt,
 )
import qualified Ouroboros.Consensus.Storage.VolatileDB as VolatileDB
import Ouroboros.Consensus.Util.CBOR (ReadIncrementalErr)
import Ouroboros.Consensus.Util.ResourceRegistry (runWithTempRegistry)
import Ouroboros.Network.Block (HeaderHash)

newtype NodeConfigError = NodeConfigError {unNodeConfigError :: Text}
  deriving (Show, Eq)

instance Exception NodeConfigError

readNodeConfig :: MonadIO m => FilePath -> m Api.NodeConfig
readNodeConfig =
  liftIO . throwExceptT . withExceptT NodeConfigError . Api.readNodeConfig . Api.NodeConfigFile

readCardanoGenesisConfig :: MonadIO m => Api.NodeConfig -> m Api.GenesisConfig
readCardanoGenesisConfig =
  liftIO . throwExceptT . Api.readCardanoGenesisConfig

readProtocolInfoCardano :: MonadIO m => FilePath -> m (ProtocolInfo IO (CardanoBlock StandardCrypto))
readProtocolInfoCardano configFilePath = do
  nodeConfig <- readNodeConfig configFilePath
  Api.mkProtocolInfoCardano <$> readCardanoGenesisConfig nodeConfig

-- TODO: Move upstream
instance Exception ReadIncrementalErr

-- | Prepare arguments for chain db.
mkDbArgs
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasLogFunc env
     , Show (Header blk)
     , Node.RunNode blk
     )
  => FilePath
  -> ProtocolInfo IO blk
  -> m (ChainDB.ChainDbArgs Identity IO blk)
mkDbArgs dbDir ProtocolInfo{pInfoInitLedger, pInfoConfig} = do
  registry <- view registryL
  let
    chainDbArgs =
      Node.mkChainDbArgs registry InFuture.dontCheck pInfoConfig pInfoInitLedger chunkInfo defArgs
  dbTracer <- mkTracer (Just "Trace") LevelDebug
  logDebug $ "Preparing to open the database: " <> displayShow dbDir
  pure $
    chainDbArgs
      { ChainDB.cdbTracer = dbTracer
      , ChainDB.cdbImmutableDbValidation = ImmutableDB.ValidateMostRecentChunk
      , ChainDB.cdbVolatileDbValidation = VolatileDB.NoValidation
      }
  where
    chunkInfo = Node.nodeImmutableDbChunkInfo (configStorage pInfoConfig)
    kParam = configSecurityParam pInfoConfig
    diskPolicy = defaultDiskPolicy kParam DefaultSnapshotInterval
    defArgs =
      ChainDB.defaultArgs (Node.stdMkChainDbHasFS dbDir) diskPolicy

withImmutableDb
  :: ( MonadUnliftIO m
     , MonadReader env m
     , GetPrevHash blk
     , ConvertRawHash blk
     , EncodeDisk blk blk
     , DecodeDisk blk (LByteString -> blk)
     , DecodeDiskDep (NestedCtxt Header) blk
     , ReconstructNestedCtxt Header blk
     , HasBinaryBlockInfo blk
     , HasLogFunc env
     )
  => ImmutableDbArgs Identity IO blk
  -> (ImmutableDB.ImmutableDB IO blk -> m b)
  -> m b
withImmutableDb iDbArgs action = do
  res <- withRunInIO $ \run ->
    ImmutableDB.withDB (ImmutableDB.openDB iDbArgs runWithTempRegistry) $ \db ->
      run $ do
        logDebug "Opened an immutable database"
        action db
  logDebug "Closed an immutable database"
  pure res

readInitLedgerState
  :: ( DecodeDisk blk (LedgerState blk)
     , DecodeDisk blk (ChainDepState (BlockProtocol blk))
     , DecodeDisk blk (AnnTip blk)
     , Serialise (HeaderHash blk)
     )
  => DiskSnapshot
  -> RIO (DbStreamerApp blk) (ExtLedgerState blk)
readInitLedgerState diskSnapshot = do
  ledgerDbFS <- ChainDB.cdbHasFSLgrDB . dsAppChainDbArgs <$> ask
  ccfg <- configCodec . pInfoConfig . dsAppProtocolInfo <$> ask
  let
    extLedgerStateDecoder =
      decodeExtLedgerState (decodeDisk ccfg) (decodeDisk ccfg) (decodeDisk ccfg)
  liftIO $
    throwExceptT $
      readSnapshot ledgerDbFS extLedgerStateDecoder decode diskSnapshot

getInitLedgerState
  :: ( DecodeDisk blk (LedgerState blk)
     , DecodeDisk blk (ChainDepState (BlockProtocol blk))
     , DecodeDisk blk (AnnTip blk)
     , Serialise (HeaderHash blk)
     )
  => Maybe DiskSnapshot
  -> RIO (DbStreamerApp blk) (ExtLedgerState blk)
getInitLedgerState = \case
  Nothing -> pInfoInitLedger . dsAppProtocolInfo <$> ask
  Just diskSnapshot -> readInitLedgerState diskSnapshot

runDbStreamerApp
  :: ( ExtLedgerState (CardanoBlock StandardCrypto)
       -> RIO (DbStreamerApp (CardanoBlock StandardCrypto)) a
     )
  -> RIO AppConfig a
runDbStreamerApp action = do
  appConf <- ask
  protocolInfo <- readProtocolInfoCardano (appConfFilePath appConf)
  dbArgs <- mkDbArgs (appConfDbDir appConf) protocolInfo
  let (iDbArgs, _, _, _) = fromChainDbArgs dbArgs
  withImmutableDb iDbArgs $ \iDb ->
    let app =
          DbStreamerApp
            { dsAppLogFunc = appConfLogFunc appConf
            , dsAppRegistry = appConfRegistry appConf
            , dsAppProtocolInfo = protocolInfo
            , dsAppChainDbArgs = dbArgs
            , dsAppIDb = iDb
            }
     in runRIO app (getInitLedgerState (appConfDiskSnapshot appConf) >>= action)
