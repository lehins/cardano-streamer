{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Streamer.RIO where

import Control.Monad.Class.MonadAsync
import Control.Monad.Class.MonadEventlog
import Control.Monad.Class.MonadFork
import Control.Monad.Class.MonadST
import Control.Monad.Class.MonadSTM
import Control.Monad.Class.MonadThrow
import Control.Monad.Class.MonadTime hiding (MonadTime (..))
import Control.Monad.Class.MonadTimer
import Data.Coerce
import NoThunks.Class
import Ouroboros.Consensus.Util.IOLike
import RIO (RIO (..), ReaderT (..), first, liftIO)

deriving instance MonadThread (RIO env)
deriving instance MonadFork (RIO env)
deriving instance MonadThrow (RIO env)
deriving instance MonadCatch (RIO env)
deriving instance MonadMask (RIO env)
deriving instance MonadST (RIO env)
deriving instance MonadSTM (RIO env)
deriving instance MonadEventlog (RIO env)
deriving instance MonadDelay (RIO env)
deriving instance MonadMonotonicTime (RIO env)
deriving instance MonadEvaluate (RIO env)
deriving via OnlyCheckWhnfNamed "RIO env a" (RIO env a) instance NoThunks (RIO env a)
deriving via
  OnlyCheckWhnfNamed "StrictTVar (RIO env) a" (StrictTVar (RIO env) a)
  instance
    NoThunks (StrictTVar (RIO env) a)
deriving via
  OnlyCheckWhnfNamed "StrictMVar (RIO env) a" (StrictMVar (RIO env) a)
  instance
    NoThunks (StrictMVar (RIO env) a)

instance MonadSTM (RIO env) where
  type STM (RIO env) = STM (ReaderT env IO)
  atomically (RIO stm) = ReaderT $ \r -> atomically (stm r)

  -- type TVar (RIO env) = TVar IO
  -- newTVar        = lift .  newTVar
  -- readTVar       = lift .  readTVar
  -- writeTVar      = lift .: writeTVar
  retry = lift retry
  orElse (RIO a) (RIO b) = RIO $ \r -> a r `orElse` b r

  -- modifyTVar     = lift .: modifyTVar
  -- modifyTVar'    = lift .: modifyTVar'
  -- stateTVar      = lift .: stateTVar
  -- swapTVar       = lift .: swapTVar
  check = lift . check

  -- type TMVar (RIO env) = TMVar IO
  -- newTMVar       = lift .  newTMVar
  -- newEmptyTMVar  = lift    newEmptyTMVar
  -- takeTMVar      = lift .  takeTMVar
  -- tryTakeTMVar   = lift .  tryTakeTMVar
  -- putTMVar       = lift .: putTMVar
  -- tryPutTMVar    = lift .: tryPutTMVar
  -- readTMVar      = lift .  readTMVar
  -- tryReadTMVar   = lift .  tryReadTMVar
  -- swapTMVar      = lift .: swapTMVar
  -- isEmptyTMVar   = lift .  isEmptyTMVar

  type TQueue (RIO env) = TQueue IO
  newTQueue = lift newTQueue
  readTQueue = lift . readTQueue
  tryReadTQueue = lift . tryReadTQueue
  peekTQueue = lift . peekTQueue
  tryPeekTQueue = lift . tryPeekTQueue
  flushTQueue = lift . flushTQueue
  writeTQueue v = lift . writeTQueue v
  isEmptyTQueue = lift . isEmptyTQueue
  unGetTQueue = lift .: unGetTQueue

  type TBQueue (RIO env) = TBQueue IO
  newTBQueue = lift . newTBQueue
  readTBQueue = lift . readTBQueue
  tryReadTBQueue = lift . tryReadTBQueue
  peekTBQueue = lift . peekTBQueue
  tryPeekTBQueue = lift . tryPeekTBQueue
  flushTBQueue = lift . flushTBQueue
  writeTBQueue = lift .: writeTBQueue
  lengthTBQueue = lift . lengthTBQueue
  isEmptyTBQueue = lift . isEmptyTBQueue
  isFullTBQueue = lift . isFullTBQueue
  unGetTBQueue = lift .: unGetTBQueue

-- type TArray (RIO env) = TArray IO

-- type TSem (RIO env) = TSem IO
-- newTSem        = lift .  newTSem
-- waitTSem       = lift .  waitTSem
-- signalTSem     = lift .  signalTSem
-- signalTSemN    = lift .: signalTSemN

-- type TChan (RIO env) = TChan IO
-- newTChan          = lift    newTChan
-- newBroadcastTChan = lift    newBroadcastTChan
-- dupTChan          = lift .  dupTChan
-- cloneTChan        = lift .  cloneTChan
-- readTChan         = lift .  readTChan
-- tryReadTChan      = lift .  tryReadTChan
-- peekTChan         = lift .  peekTChan
-- tryPeekTChan      = lift .  tryPeekTChan
-- writeTChan        = lift .: writeTChan
-- unGetTChan        = lift .: unGetTChan
-- isEmptyTChan      = lift .  isEmptyTChan

newtype AsyncRIO env a = AsyncRIO {getAsyncRIO :: Async IO a}

instance MonadAsync (RIO env) where
  type Async (RIO env) = AsyncRIO env
  async m = async (coerce m)
  asyncBound m = asyncBound (coerce m)
  asyncOn i m = asyncOn i (coerce m)
  asyncThreadId (AsyncRIO a) = asyncThreadId a
  withAsync m f = withAsync (coerce m) (coerce f)
  withAsyncBound m f = withAsyncBound (coerce m) (coerce f)
  withAsyncOn m f = withAsyncOn (coerce m) (coerce f)

  -- asyncWithUnmask m f = asyncWithUnmask (coerce m) (coerce f)

  -- asyncWithUnmask f = RIO $ ReaderT $ \r -> fmap AsyncRIO $
  --   asyncWithUnmask $
  --     \unmask -> runRIO (f (liftF unmask)) r
  --   where
  --     liftF :: (IO a -> IO a) -> RIO env a -> RIO env a
  --     liftF g (RIO r) = RIO (g . r)

  -- asyncOnWithUnmask n f = RIO $ \r -> fmap AsyncRIO $
  --   asyncOnWithUnmask n $
  --     \unmask -> runRIO (f (liftF unmask)) r
  --   where
  --     liftF :: (IO a -> IO a) -> RIO env a -> RIO env a
  --     liftF g (RIO r) = RIO (g . r)

  -- withAsyncWithUnmask action f =
  --   RIO $ \r -> withAsyncWithUnmask
  --     ( \unmask -> case action (liftF unmask) of
  --         RIO ma -> ma r
  --     )
  --     $ \a -> runRIO (f (AsyncRIO a)) r
  --   where
  --     liftF :: (IO a -> IO a) -> RIO env a -> RIO env a
  --     liftF g (RIO r) = RIO (g . r)

  -- withAsyncOnWithUnmask n action f =
  --   RIO $ \r -> withAsyncOnWithUnmask
  --     n
  --     ( \unmask -> case action (liftF unmask) of
  --         RIO ma -> ma r
  --     )
  --     $ \a -> runRIO (f (AsyncRIO a)) r
  --   where
  --     liftF :: (IO a -> IO a) -> RIO env a -> RIO env a
  --     liftF g (RIO r) = RIO (g . r)

  -- waitCatchSTM = lift . waitCatchSTM . getAsyncRIO
  -- pollSTM = lift . pollSTM . getAsyncRIO

  -- race (RIO ma) (RIO mb) = RIO $ \r -> race (ma r) (mb r)
  -- race_ (RIO ma) (RIO mb) = RIO $ \r -> race_ (ma r) (mb r)
  -- concurrently (RIO ma) (RIO mb) = RIO $ \r -> concurrently (ma r) (mb r)

  wait = liftIO . wait . getAsyncRIO
  poll = liftIO . poll . getAsyncRIO
  waitCatch = liftIO . waitCatch . getAsyncRIO
  cancel = liftIO . cancel . getAsyncRIO
  uninterruptibleCancel = liftIO . uninterruptibleCancel . getAsyncRIO
  cancelWith = (liftIO .: cancelWith) . getAsyncRIO
  waitAny = fmap (first AsyncRIO) . liftIO . waitAny . map getAsyncRIO
  waitAnyCatch = fmap (first AsyncRIO) . liftIO . waitAnyCatch . map getAsyncRIO
  waitAnyCancel = fmap (first AsyncRIO) . liftIO . waitAnyCancel . map getAsyncRIO
  waitAnyCatchCancel = fmap (first AsyncRIO) . liftIO . waitAnyCatchCancel . map getAsyncRIO
  waitEither = on (liftIO .: waitEither) getAsyncRIO
  waitEitherCatch = on (liftIO .: waitEitherCatch) getAsyncRIO
  waitEitherCancel = on (liftIO .: waitEitherCancel) getAsyncRIO
  waitEitherCatchCancel = on (liftIO .: waitEitherCatchCancel) getAsyncRIO
  waitEither_ = on (liftIO .: waitEither_) getAsyncRIO
  waitBoth = on (liftIO .: waitBoth) getAsyncRIO

(.:) :: (c -> d) -> (a -> b -> c) -> (a -> b -> d)
(f .: g) x y = f (g x y)
on ::
  (f a -> f b -> c) ->
  (forall x. g x -> f x) ->
  (g a -> g b -> c)
on f g = \a b -> f (g a) (g b)

-- instance IOLike (RIO env) where
--   forgetSignKeyKES = liftIO . forgetSignKeyKES
