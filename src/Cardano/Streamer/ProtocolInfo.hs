{-# LANGUAGE FlexibleContexts #-}
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

readInitLedgerState
  :: ( DecodeDisk blk (LedgerState blk)
     , DecodeDisk blk (ChainDepState (BlockProtocol blk))
     , DecodeDisk blk (AnnTip blk)
     , Serialise (HeaderHash blk)
     )
  => DbConfig blk
  -> DiskSnapshot
  -> IO (ExtLedgerState blk)
readInitLedgerState DbConfig{dbConfChainDbArgs, dbConfProtocolInfo} diskSnapshot = do
  let
    ledgerDbFS = ChainDB.cdbHasFSLgrDB dbConfChainDbArgs
    ccfg = configCodec (pInfoConfig dbConfProtocolInfo)
    extLedgerStateDecoder =
      decodeExtLedgerState (decodeDisk ccfg) (decodeDisk ccfg) (decodeDisk ccfg)
  throwExceptT $
    readSnapshot ledgerDbFS extLedgerStateDecoder decode diskSnapshot

mkIDbArgs
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasLogFunc env
     , Node.RunNode blk
     , Show (Header blk)
     )
  => FilePath
  -> ProtocolInfo IO blk
  -> m (ImmutableDbArgs Identity IO blk)
mkIDbArgs dbDir protocolInfo = do
  (iDbArgs, _, _, _) <- fromChainDbArgs . dbConfChainDbArgs <$> mkDbConfig dbDir protocolInfo
  pure iDbArgs

data DbConfig blk = DbConfig
  { dbConfDir :: !FilePath
  , dbConfChainDbArgs :: !(ChainDB.ChainDbArgs Identity IO blk)
  , dbConfProtocolInfo :: !(ProtocolInfo IO blk)
  }

-- | Prepare arguments for chain db.
mkDbConfig
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasLogFunc env
     , Show (Header blk)
     , Node.RunNode blk
     )
  => FilePath
  -> ProtocolInfo IO blk
  -> m (DbConfig blk)
mkDbConfig dbDir protocolInfo@ProtocolInfo{pInfoInitLedger, pInfoConfig} = do
  registry <- view registryL
  let
    chainDbArgs =
      Node.mkChainDbArgs registry InFuture.dontCheck pInfoConfig pInfoInitLedger chunkInfo defArgs
  dbTracer <- mkTracer LevelInfo
  logDebug $ "Preparing to open the database: " <> displayShow dbDir
  pure
    DbConfig
      { dbConfDir = dbDir
      , dbConfChainDbArgs =
          chainDbArgs
            { ChainDB.cdbTracer = dbTracer
            }
      , dbConfProtocolInfo = protocolInfo
      }
  where
    chunkInfo = Node.nodeImmutableDbChunkInfo (configStorage pInfoConfig)
    kParam = configSecurityParam pInfoConfig
    diskPolicy = defaultDiskPolicy kParam DefaultSnapshotInterval
    defArgs =
      ChainDB.defaultArgs (Node.stdMkChainDbHasFS dbDir) diskPolicy

withImmutableDb
  :: ( MonadReader env m
     , MonadIO m
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
  -> (ImmutableDB.ImmutableDB IO blk -> ReaderT env IO b)
  -> m b
withImmutableDb iDbArgs action = do
  env <- ask
  res <- liftIO $ ImmutableDB.withDB (ImmutableDB.openDB iDbArgs runWithTempRegistry) $ \db ->
    flip runReaderT env $ do
      logDebug "Opened an immutable database"
      action db
  logDebug "Closed an immutable database"
  pure res
