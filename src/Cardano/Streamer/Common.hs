{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Cardano.Streamer.Common (
  DbStreamerApp (..),
  AppConfig (..),
  Opts (..),
  Command (..),
  throwExceptT,
  throwShowExceptT,
  throwStringExceptT,
  mkTracer,
  ValidationMode (..),
  HasImmutableDb (..),
  HasResourceRegistry (..),
  parseStakingCredential,
  RIO,
  runRIO,
  module X,
) where

import qualified Cardano.Address as A
import qualified Cardano.Address.Style.Shelley as A
import Cardano.Crypto.Hash.Class (hashFromBytes)
import Cardano.Ledger.BaseTypes (SlotNo (..))
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Hashes
import Cardano.Ledger.Keys
import Control.Monad.Trans.Except
import Control.Tracer (Tracer (..))
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import Ouroboros.Consensus.Util.ResourceRegistry (ResourceRegistry)
import RIO as X hiding (RIO, runRIO)

type RIO env = ReaderT env IO

runRIO :: MonadIO m => r -> RIO r a -> m a
runRIO env = liftIO . flip runReaderT env

deriving instance Display SlotNo

class HasResourceRegistry env where
  registryL :: Lens' env (ResourceRegistry IO)

mkTracer
  :: (MonadReader env m1, MonadIO m2, HasLogFunc env, Show a)
  => Maybe Text
  -- ^ Optional prefix for tracing messages
  -> LogLevel
  -> m1 (Tracer m2 a)
mkTracer mPrefix logLevel = do
  logFunc <- view logFuncL
  return $ Tracer $ \ev ->
    let msg =
          case mPrefix of
            Just prefix -> "[" <> display prefix <> "] " <> displayShow ev
            Nothing -> displayShow ev
     in flip runReaderT logFunc $ logGeneric mempty logLevel msg

throwExceptT :: (Exception e, MonadIO m) => ExceptT e m a -> m a
throwExceptT m =
  runExceptT m >>= \case
    Left exc -> throwIO exc
    Right res -> pure res

throwStringExceptT :: (HasCallStack, MonadIO m) => ExceptT String m a -> m a
throwStringExceptT m =
  runExceptT m >>= \case
    Left exc -> throwString exc
    Right res -> pure res

throwShowExceptT :: (HasCallStack, Show e, MonadIO m) => ExceptT e m a -> m a
throwShowExceptT m = throwStringExceptT $ withExceptT show m

data ValidationMode
  = FullValidation
  | ReValidation
  | NoValidation
  deriving (Eq, Ord, Enum, Bounded, Show)

data DbStreamerApp blk = DbStreamerApp
  { dsAppLogFunc :: !LogFunc
  , dsAppRegistry :: !(ResourceRegistry IO)
  , dsAppProtocolInfo :: !(ProtocolInfo blk)
  , dsAppChainDbArgs :: !(ChainDB.ChainDbArgs Identity IO blk)
  , dsAppIDb :: !(ImmutableDB.ImmutableDB IO blk)
  , dsAppOutDir :: !(Maybe FilePath)
  -- ^ Output directory where to write files
  , dsStopSlotNo :: !(Maybe SlotNo)
  -- ^ Last slot number to execute
  , dsValidationMode :: !ValidationMode
  }

class HasImmutableDb env blk | env -> blk where
  iDbL :: Lens' env (ImmutableDB.ImmutableDB IO blk)

instance HasImmutableDb (DbStreamerApp blk) blk where
  iDbL = lens dsAppIDb $ \app iDb -> app{dsAppIDb = iDb}

instance HasLogFunc (DbStreamerApp blk) where
  logFuncL = lens dsAppLogFunc $ \app logFunc -> app{dsAppLogFunc = logFunc}

instance HasResourceRegistry (DbStreamerApp blk) where
  registryL = lens dsAppRegistry $ \app registry -> app{dsAppRegistry = registry}

data AppConfig = AppConfig
  { appConfDbDir :: !FilePath
  -- ^ Database directory
  , appConfFilePath :: !FilePath
  -- ^ Config file path
  , appConfReadDiskSnapshot :: !(Maybe DiskSnapshot)
  , appConfWriteDiskSnapshots :: ![DiskSnapshot]
  , appConfStopSlotNumber :: !(Maybe Word64)
  , appConfValidationMode :: !ValidationMode
  , appConfLogFunc :: !LogFunc
  , appConfRegistry :: !(ResourceRegistry IO)
  }

instance HasLogFunc AppConfig where
  logFuncL = lens appConfLogFunc $ \app logFunc -> app{appConfLogFunc = logFunc}

instance HasResourceRegistry AppConfig where
  registryL = lens appConfRegistry $ \app registry -> app{appConfRegistry = registry}

data Command
  = Replay
  | Benchmark
  | ComputeRewards (NonEmpty (Credential 'Staking StandardCrypto))
  deriving (Show)

--    Set

data Opts = Opts
  { oChainDir :: FilePath
  -- ^ Db directory
  , oConfigFilePath :: FilePath
  -- ^ Config file path
  , oOutDir :: Maybe FilePath
  -- ^ Directory where requested files should be written to.
  , oSnapShotSuffix :: Maybe String
  -- ^ Where to start from
  , oReadSnapShotSlotNumber :: Maybe Word64
  -- ^ Slot number expected for the snapshot to read from.
  , oWriteSnapShotSlotNumbers :: [Word64]
  -- ^ Slot numbers for creating snapshots
  , oStopSlotNumber :: Maybe Word64
  -- ^ Stope replaying the chain once reaching this slot number. When no slot number is
  -- supplied replay will stop only at the end of the immutable chain.
  , oValidationMode :: ValidationMode
  -- ^ What is the level of ledger validation
  , oLogLevel :: LogLevel
  -- ^ Minimum log level
  , oVerbose :: Bool
  -- ^ Verbose logging?
  , oDebug :: Bool
  -- ^ Debug logging?
  , oCommand :: Command
  }
  deriving (Show)

parseStakingCredential :: MonadFail m => Text -> m (Credential 'Staking StandardCrypto)
parseStakingCredential txt =
  case A.fromBech32 txt of
    Nothing -> fail "Can't parse as Bech32 Address"
    Just addr ->
      case A.eitherInspectAddress Nothing addr of
        Left err -> fail $ show err
        Right (A.InspectAddressByron{}) -> fail "Byron Address can't have Staking Crednetial"
        Right (A.InspectAddressIcarus{}) -> fail "Icarus Address can't have Staking Crednetial"
        Right (A.InspectAddressShelley ai) ->
          maybe (fail $ "Address does not contain a Staking Credential: " ++ show txt) pure $
            (KeyHashObj . KeyHash . partialHash <$> A.infoStakeKeyHash ai)
              <|> (ScriptHashObj . ScriptHash . partialHash <$> A.infoStakeScriptHash ai)
  where
    partialHash bs =
      case hashFromBytes bs of
        Nothing -> error $ "Impossible: This should be a valid Hash value: " ++ show bs
        Just h -> h
