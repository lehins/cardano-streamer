{-# LANGUAGE LambdaCase #-}

module Cardano.Streamer.Common (
  throwExceptT,
  mkTracer,
  HasResourceRegistry (..),
  RIO,
  module X,
) where

import Control.Monad.Trans.Except
import Control.Tracer (Tracer (..))
import Ouroboros.Consensus.Util.ResourceRegistry
import RIO as X hiding (RIO)

type RIO env = ReaderT env IO

class HasResourceRegistry env where
  registryL :: Lens' env (ResourceRegistry IO)

mkTracer
  :: (MonadReader env m1, MonadIO m2, HasLogFunc env, Show a)
  => LogLevel
  -> m1 (Tracer m2 a)
mkTracer logLevel = do
  logFunc <- view logFuncL
  return $ Tracer $ \ev ->
    flip runReaderT logFunc $ logGeneric mempty logLevel $ displayShow ev

throwExceptT :: (Exception e, MonadIO m) => ExceptT e m a -> m a
throwExceptT m =
  runExceptT m >>= \case
    Left exc -> throwIO exc
    Right res -> pure res
