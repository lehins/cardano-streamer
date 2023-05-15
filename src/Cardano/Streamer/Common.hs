{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Cardano.Streamer.Common (
  DbStreamerApp (..),
  AppConfig (..),
  throwExceptT,
  throwShowExceptT,
  throwStringExceptT,
  mkTracer,
  HasImmutableDb (..),
  HasResourceRegistry (..),
  RIO,
  runRIO,
  module X,
) where

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

data DbStreamerApp blk = DbStreamerApp
  { dsAppLogFunc :: !LogFunc
  , dsAppRegistry :: !(ResourceRegistry IO)
  , dsAppProtocolInfo :: !(ProtocolInfo IO blk)
  , dsAppChainDbArgs :: !(ChainDB.ChainDbArgs Identity IO blk)
  , dsAppIDb :: !(ImmutableDB.ImmutableDB IO blk)
  , dsAppOutDir :: !(Maybe FilePath)
  -- ^ Output directory where to write files
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
  , appConfDiskSnapshot :: !(Maybe DiskSnapshot)
  , appConfLogFunc :: !LogFunc
  , appConfRegistry :: !(ResourceRegistry IO)
  }

instance HasLogFunc AppConfig where
  logFuncL = lens appConfLogFunc $ \app logFunc -> app{appConfLogFunc = logFunc}

instance HasResourceRegistry AppConfig where
  registryL = lens appConfRegistry $ \app registry -> app{appConfRegistry = registry}
