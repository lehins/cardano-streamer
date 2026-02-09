{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Cardano.Streamer.Storage (
  mkLedgerDbArgs,
  openLedgerDb,
  ledgerDbExtLedgerStateForBlock,
  ledgerDbTipExtLedgerState,
  ledgerDbWithOriginAnnTip,
  ledgerDbFlushExtLedgerState,
  ledgerDbStoreSnapshot,
  LedgerDbBackend (..),
  ledgerDbStateWithTablesForBlock,
  withImmutableDb,
)
where

import Cardano.Slotting.Slot (SlotNo (..), WithOrigin (..))
import Cardano.Streamer.Common
import Cardano.Streamer.Measure (measureAction)
import Control.ResourceRegistry (runWithTempRegistry)
import qualified Data.SOP.Dict as Dict
import Data.Typeable
import Ouroboros.Consensus.Block (ConvertRawHash, GetPrevHash, Point (..))
import Ouroboros.Consensus.Block.NestedContent (NestedCtxt)
import Ouroboros.Consensus.Cardano.Block (Header)
import Ouroboros.Consensus.HardFork.Abstract (HasHardForkHistory)
import Ouroboros.Consensus.HeaderValidation (AnnTip, headerStateTip)
import Ouroboros.Consensus.Ledger.Abstract (ApplyBlock (getBlockKeySets))
import Ouroboros.Consensus.Ledger.Basics (IsLedger, LedgerState)
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerState (headerState))
import Ouroboros.Consensus.Ledger.SupportsProtocol (LedgerSupportsProtocol)
import Ouroboros.Consensus.Ledger.Tables (withLedgerTables)
import Ouroboros.Consensus.Ledger.Tables.Basics (LedgerTables)
import Ouroboros.Consensus.Ledger.Tables.MapKind (DiffMK, EmptyMK, ValuesMK)
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.ImmutableDB.Impl (ImmutableDbArgs)
import qualified Ouroboros.Consensus.Storage.LedgerDB as LDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1 as LDB.V1
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.Args as LDB.V1.Args
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.BackingStore.Impl.LMDB as LDB.V1.LMDB
import qualified Ouroboros.Consensus.Storage.LedgerDB.V1.Snapshots as LDB.V1.Snapshots
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2 as LDB.V2
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.Args as LDB.V2.Args
import qualified Ouroboros.Consensus.Storage.LedgerDB.V2.InMemory as LDB.V2.InMemory
import Ouroboros.Consensus.Storage.Serialisation (
  DecodeDisk,
  DecodeDiskDep,
  EncodeDisk,
  HasBinaryBlockInfo,
  ReconstructNestedCtxt,
 )
import qualified Ouroboros.Consensus.Util.IOLike as IOLike
import Ouroboros.Network.Block (pointSlot)
import System.FS.API (SomeHasFS (..), createDirectoryIfMissing, mkFsPath)

-- | Similar to `LDB.openDbInternal`, except open LedgerDb without any replay or modifcations to any
-- of the existing snasphots.
openLedgerDbNoReplay ::
  ( LedgerSupportsProtocol blk
  , HasLogFunc env
  , HasCallStack
  ) =>
  -- | Ledger Db args
  LDB.LedgerDbArgs Identity IO blk ->
  -- | Db initializer
  LDB.InitDB db IO blk ->
  RIO env (LDB.LedgerDB' IO blk, LDB.TestInternals' IO blk)
openLedgerDbNoReplay ldbArgs initDb = do
  case hasFS of
    SomeHasFS fs -> liftIO $ createDirectoryIfMissing fs True (mkFsPath [])
  db <- case lgrStartSnapshot of
    Nothing -> liftIO $ initFromGenesis
    Just diskSnapshot -> do
      logInfo $ "Reading initial ledger state: " <> display diskSnapshot
      measureAction (liftIO $ initFromSnapshot diskSnapshot) >>= \case
        (_, Left err) ->
          error (show err)
        (measure, Right (db, _pt)) -> do
          logInfo $
            "Done reading initial Ledger State snapshot in: " <> display measure
          pure db
  (ledgerDb, internal) <- liftIO $ LDB.mkLedgerDb initDb db
  return (ledgerDb, internal)
  where
    LDB.InitDB{LDB.initFromGenesis, LDB.initFromSnapshot} = initDb
    LDB.LedgerDbArgs
      { LDB.lgrStartSnapshot
      , LDB.lgrHasFS = hasFS
      } = ldbArgs

openLedgerDb ::
  forall blk env.
  ( LedgerSupportsProtocol blk
  , HasHardForkHistory blk
  , LDB.LedgerSupportsLedgerDB blk
  , HasLogFunc env
  ) =>
  LDB.LedgerDbArgs Identity IO blk -> RIO env (LedgerDb blk)
openLedgerDb ldbArgs = do
  let
    getBlock _ = pure (error "No getBlock")
    getVolatileSuffix = LDB.praosGetVolatileSuffix $ LDB.ledgerDbCfgSecParam $ LDB.lgrConfig ldbArgs
  logInfo "Initializing LedgerDb"
  (ledgerDb, testInternals) <-
    case LDB.lgrFlavorArgs ldbArgs of
      LDB.LedgerDbFlavorArgsV1 ldbBackendArgs -> do
        let
          snapManager = LDB.V1.Snapshots.snapshotManager ldbArgs
          initDb = LDB.V1.mkInitDb ldbArgs ldbBackendArgs getBlock snapManager getVolatileSuffix
        openLedgerDbNoReplay ldbArgs initDb
      LDB.LedgerDbFlavorArgsV2 (LDB.V2.Args.V2Args LDB.V2.Args.InMemoryHandleArgs) -> do
        let
          snapManager = LDB.V2.InMemory.snapshotManager ldbArgs
          ldbBackendArgs :: LDB.V2.Args.HandleEnv IO
          ldbBackendArgs = LDB.V2.Args.InMemoryHandleEnv
          initDb = LDB.V2.mkInitDb ldbArgs ldbBackendArgs getBlock snapManager getVolatileSuffix
        openLedgerDbNoReplay ldbArgs initDb
      LDB.LedgerDbFlavorArgsV2 args -> error $ "Unsupported args: " <> show (typeOf args)
  logInfo $ "Done initializing LedgerDb"
  pure $!
    LedgerDb
      { lLedgerDb = ledgerDb
      , lTestInternals = testInternals
      }

ledgerDbStateWithTablesForBlock ::
  (HasLedgerDb env blk, LedgerSupportsProtocol blk) =>
  blk ->
  RIO env (ExtLedgerState blk EmptyMK, LedgerTables (ExtLedgerState blk) ValuesMK)
ledgerDbStateWithTablesForBlock block = do
  ledgerDb <- view ledgerDbL
  liftIO $ LDB.withPrivateTipForker (lLedgerDb ledgerDb) $ \tipForker -> do
    st <- IOLike.atomically $ LDB.forkerGetLedgerState tipForker
    tbs <- LDB.forkerReadTables tipForker (getBlockKeySets block)
    pure (st, tbs)

ledgerDbTipExtLedgerState ::
  (MonadIO m, MonadReader env m, HasLedgerDb env blk) => m (ExtLedgerState blk EmptyMK)
ledgerDbTipExtLedgerState = do
  LedgerDb{lLedgerDb} <- view ledgerDbL
  liftIO (IOLike.atomically (LDB.getVolatileTip lLedgerDb))

ledgerDbWithOriginAnnTip ::
  (MonadIO m, MonadReader env m, HasLedgerDb env blk) => m (WithOrigin (AnnTip blk))
ledgerDbWithOriginAnnTip = headerStateTip . headerState <$> ledgerDbTipExtLedgerState

ledgerDbExtLedgerStateForBlock ::
  ( MonadIO m
  , MonadReader env m
  , HasLedgerDb env blk
  , LedgerSupportsProtocol blk
  ) =>
  blk -> m (ExtLedgerState blk ValuesMK)
ledgerDbExtLedgerStateForBlock block = do
  LedgerDb{lLedgerDb} <- view ledgerDbL
  liftIO $ LDB.withPrivateTipForker lLedgerDb $ \tipForker -> do
    extLedgerState <- IOLike.atomically $ LDB.forkerGetLedgerState tipForker
    tables <- LDB.forkerReadTables tipForker (getBlockKeySets block)
    pure $ extLedgerState `withLedgerTables` tables

ledgerDbFlushExtLedgerState ::
  ( MonadIO m
  , MonadReader env m
  , HasLedgerDb env blk
  ) =>
  ExtLedgerState blk DiffMK -> m ()
ledgerDbFlushExtLedgerState extLedgerState = do
  LedgerDb{lLedgerDb, lTestInternals} <- view ledgerDbL
  liftIO $ LDB.push lTestInternals extLedgerState >> LDB.tryFlush lLedgerDb

ledgerDbStoreSnapshot ::
  forall env blk m.
  ( MonadIO m
  , HasLogFunc env
  , MonadReader env m
  , HasLedgerDb env blk
  , IsLedger (LedgerState blk)
  ) =>
  DiskSnapshot -> m ()
ledgerDbStoreSnapshot DiskSnapshot{dsNumber, dsSuffix} = do
  LedgerDb{lLedgerDb, lTestInternals} <- view ledgerDbL
  liftIO (IOLike.atomically (pointSlot <$> LDB.currentPoint lLedgerDb)) >>= \case
    Origin -> logError "Requested to create a SnapShot at Origin"
    At (SlotNo slotNo)
      | slotNo /= dsNumber ->
          logError $
            "Requested to create a SnapShot at SlotNo "
              <> display dsNumber
              <> " while LedgerDb is at: "
              <> display slotNo
      | otherwise ->
          liftIO $ LDB.takeSnapshotNOW lTestInternals LDB.TakeAtVolatileTip dsSuffix

data LedgerDbBackend
  = InMemV1
  | LMDBV1
  | InMemV2

-- \| LSMV2

mkLedgerDbArgs :: LedgerDbBackend -> LDB.LedgerDbFlavorArgs Identity IO
mkLedgerDbArgs ldbBackend =
  case ldbBackend of
    InMemV1 ->
      LDB.LedgerDbFlavorArgsV1 $
        LDB.V1.Args.V1Args
          LDB.V1.Args.DisableFlushing
          LDB.V1.Args.InMemoryBackingStoreArgs
    LMDBV1 ->
      let
        defaultLMDBLimits :: LDB.V1.LMDB.LMDBLimits
        defaultLMDBLimits =
          LDB.V1.LMDB.LMDBLimits
            { LDB.V1.LMDB.lmdbMapSize = 16 * 1024 * 1024 * 1024
            , LDB.V1.LMDB.lmdbMaxDatabases = 10
            , LDB.V1.LMDB.lmdbMaxReaders = 16
            }
       in
        LDB.LedgerDbFlavorArgsV1 $
          LDB.V1.Args.V1Args LDB.V1.Args.DisableFlushing $
            LDB.V1.Args.LMDBBackingStoreArgs "lmdb" defaultLMDBLimits Dict.Dict
    InMemV2 ->
      LDB.LedgerDbFlavorArgsV2 $ LDB.V2.Args.V2Args LDB.V2.Args.InMemoryHandleArgs

-- LSMV2 ->
--   LDB.LedgerDbBackendArgsV2 $
--     LDB.V2.SomeBackendArgs $
--       LSM.LSMArgs (mkFsPath ["lsm"]) lsmSalt (LSM.stdMkBlockIOFS dbDir)

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
  -- | Slot number where streaming of blocks will start from.
  Maybe SlotNo ->
  (ImmutableDB.ImmutableDB IO blk -> Point blk -> m b) ->
  m b
withImmutableDb iDbArgs mStartSlotNo action = do
  result <- withRunInIO $ \run ->
    bracket
      (ImmutableDB.openDBInternal iDbArgs runWithTempRegistry)
      (ImmutableDB.closeDB . fst)
      $ \(iDb, iDbInternal) -> do
        run $ logDebug "Opened an immutable database"
        startPoint <- case mStartSlotNo of
          Nothing -> pure GenesisPoint
          Just slotNo -> do
            ImmutableDB.getHashForSlot iDbInternal slotNo >>= \case
              Just hash -> pure $ BlockPoint slotNo hash
              Nothing ->
                throwString $
                  "There is no block with the supplied slot number " <> show slotNo <> " in the ImmutableDB"
        run $ action iDb startPoint
  result <$ logDebug "Closed an immutable database"
