{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Cardano.Streamer.Producer where

import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary.Plain (decodeFullDecoder)
import Cardano.Ledger.Rules.ValidationMode (lblStatic)
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.Inspection
import Cardano.Streamer.LedgerState
import Cardano.Streamer.RTS
import Cardano.Streamer.Storage
import Cardano.Streamer.Time
import Conduit
import Control.Monad.Trans.Except
import Control.State.Transition.Extended
import qualified Data.ByteString.Lazy as BSL
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (configCodec)
import Ouroboros.Consensus.Ledger.Abstract (
  applyBlockLedgerResultWithValidation,
 )
import Ouroboros.Consensus.Ledger.Basics (IsLedger, LedgerResult (..), applyChainTickLedgerResult)
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState)
import Ouroboros.Consensus.Ledger.Tables.MapKind (ValuesMK)
import Ouroboros.Consensus.Ledger.Tables.Utils (applyDiffs, forgetLedgerTables, prependDiffs)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Shelley.Ledger.Block (ShelleyBlock (..))
import Ouroboros.Consensus.Shelley.Ledger.Ledger ()
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import Ouroboros.Consensus.Storage.Serialisation (DecodeDisk (decodeDisk))
import RIO.FilePath
import RIO.List as List
import qualified RIO.Set as Set
import qualified RIO.Text as T

sourceBlocks ::
  forall blk i b m.
  ( MonadIO m
  , BlockHeaderHash blk
  , MonadReader (DbStreamerApp blk) m
  , HasHeader blk
  ) =>
  BlockComponent blk b ->
  ConduitT i (BlockWithInfo b) m ()
sourceBlocks blockComponent = do
  stratPoint <- dsAppStartPoint <$> ask
  registry <- view registryL
  iDb <- view iDbL
  let blockComponents =
        (,,,,) <$> GetSlot <*> GetBlockSize <*> GetHeaderSize <*> GetHash <*> blockComponent
  itr <-
    liftIO $
      ImmutableDB.streamAfterKnownPoint iDb registry blockComponents stratPoint
  mStopSlotNo <- dsAppStopSlotNo <$> ask
  let yieldBlockWithInfo numBlocksProcessed components = do
        let (slotNo, blockSize, blockHeaderSize, blockHeaderHash, component) = components
        yield $!
          BlockWithInfo
            { biSlotNo = slotNo
            , biBlockSize = blockSize
            , biBlocksProcessed = numBlocksProcessed
            , biBlockHeaderSize = blockHeaderSize
            , biBlockHeaderHash = toHeaderHash (Proxy @blk) blockHeaderHash
            , biBlockComponent = component
            }
        writeStreamerStats slotNo
  case mStopSlotNo of
    Nothing ->
      flip fix 1 $ \loop numBlocksProcessed ->
        liftIO (ImmutableDB.iteratorNext itr) >>= \case
          ImmutableDB.IteratorExhausted -> pure ()
          ImmutableDB.IteratorResult bwi -> do
            yieldBlockWithInfo numBlocksProcessed bwi
            loop (numBlocksProcessed + 1)
    Just stopSlotNo ->
      flip fix 1 $ \loop numBlocksProcessed ->
        liftIO (ImmutableDB.iteratorNext itr) >>= \case
          ImmutableDB.IteratorExhausted -> pure ()
          ImmutableDB.IteratorResult (slotNo, _, _, _, _)
            | slotNo > stopSlotNo ->
                logInfo $
                  "Stopped right before processing slot number "
                    <> display slotNo
                    <> " because hard stop was requested at slot number "
                    <> display stopSlotNo
          ImmutableDB.IteratorResult bwi -> do
            yieldBlockWithInfo numBlocksProcessed bwi
            loop (numBlocksProcessed + 1)

sourceBlocksWithInspector ::
  BlockComponent (CardanoBlock StandardCrypto) b ->
  SlotInspector b r ->
  ConduitT a r (RIO App) ()
sourceBlocksWithInspector blockComponent slotInspector = do
  -- Remove any slot numbers that are in the past and sort
  initExtLedgerState <- ledgerDbTipExtLedgerState
  refDiskSnapshots <- dsAppWriteDiskSnapshots <$> ask
  modifyIORef' refDiskSnapshots $ \ds ->
    sortOn dsNumber $
      case tipSlotNo <$> tipFromExtLedgerState initExtLedgerState of
        Nothing -> ds
        Just (SlotNo initSlotNo) -> List.filter ((>= initSlotNo) . dsNumber) ds
  -- This is useful for when there is no desire to process any blocks and only initial snapshot
  -- conversion is needed
  diskSnapshotsWriter initExtLedgerState
  let
    blockComponentWithRawBlock = (,) <$> GetRawBlock <*> blockComponent
  sourceBlocks blockComponentWithRawBlock
    .| mapMC (advanceSlot slotInspector)

sourceBlocksWithInspector_ :: SlotInspector () r -> ConduitT a r (RIO App) ()
sourceBlocksWithInspector_ = sourceBlocksWithInspector (GetPure ())

advanceSlot ::
  SlotInspector b r ->
  BlockWithInfo (LByteString, b) ->
  RIO App r
advanceSlot (SlotInspector SlotInspection{..}) !bwi = do
  app <- ask
  let infoConfig = pInfoConfig $ dsAppProtocolInfo app
      blockBytes = fst (biBlockComponent bwi)
      mStopSlotNo = dsAppStopSlotNo app
  (a, !block) <- siDecodeBlock bwi $ do
    case decodeFullDecoder "Block" (decodeDisk (configCodec infoConfig)) blockBytes of
      Right decBlock -> pure $! decBlock blockBytes
      Left err -> do
        writeBlockWithState (fst <$> bwi) Nothing Nothing
        throwString $ show err
  let
    slotNo = biSlotNo bwi
    era = blockCardanoEra block
  blocksWriter (fst <$> bwi)
  (b, !prevExtLedgerState) <- siLedgerDbLookup a $ ledgerDbExtLedgerStateForBlock block
  let
    extLedgerConfig = ExtLedgerCfg infoConfig
    epochNo = extLedgerStateEpochNo prevExtLedgerState
    logStickyStatus = do
      elapsedTime <- getElapsedTime
      logSticky $
        "["
          <> displayShow era
          <> ": EpochNo "
          <> display (unEpochNo epochNo)
          <> " - "
          <> displayShow slotNo
          <> maybe mempty (\s -> "/" <> display s) mStopSlotNo
          <> "] Blocks: "
          <> display (biBlocksProcessed bwi)
          <> " - Elapsed "
          <> display (T.pack (showTime Nothing False elapsedTime))
    reportValidationError errorMessage = do
      logStickyStatus
      logError $
        "Encountered an error at slot: "
          <> display slotNo
          <> " with block: "
          <> display (biBlockHeaderHash bwi)
      writeBlockWithState (fst <$> bwi) (Just block) (Just prevExtLedgerState)
      throwString $ show errorMessage
    reportException :: Text -> RIO App a -> RIO App a
    reportException name =
      flip withException $ \(exc :: SomeException) -> do
        when (isSyncException exc) $ do
          logError $ "Received an exception in " <> display name <> ": " <> displayShow exc
          reportValidationError exc
  (c, !tickExtLedgerState) <- siTick b $ \enableTickEvents -> reportException "TICK" $ do
    pure $!
      applyChainTickLedgerResult enableTickEvents extLedgerConfig slotNo $
        forgetLedgerTables prevExtLedgerState
  (d, !newExtLedgerState) <-
    siApplyBlock c $ \enableBlockEvents -> reportException "BBODY" $ do
      let validation =
            case dsAppValidationMode app of
              FullValidation -> ValidateAll
              ReValidation -> ValidateSuchThat (notElem lblStatic)
              NoValidation -> ValidateNone
          extLedgerState = applyDiffs prevExtLedgerState tickExtLedgerState
          applyBlock =
            applyBlockLedgerResultWithValidation
              validation
              enableBlockEvents
              extLedgerConfig
              block
              extLedgerState
      -- when (slotNo > SlotNo 16688800) $
      --   throwString "Simluating validation failure"
      case runExcept applyBlock of
        Right lrBlock -> pure $! prependDiffs tickExtLedgerState (lrResult lrBlock)
        Left errorMessage -> do
          logStickyStatus
          reportValidationError errorMessage
  e <- siLedgerDbPush d $ ledgerDbFlushExtLedgerState newExtLedgerState
  when (biBlocksProcessed bwi `mod` 20 == 0) logStickyStatus
  diskSnapshotsWriter newExtLedgerState
  let !slot =
        SlotWithBlock
          { swbRawBlock = fst $ biBlockComponent bwi
          , swbBlockWithInfo = block <$ bwi
          , swbPrevExtLedgerState = prevExtLedgerState
          , swbTickExtLedgerState = tickExtLedgerState
          , swbNewExtLedgerState = newExtLedgerState
          }
  siFinal slot a b c d e

blocksWriter ::
  (MonadReader (DbStreamerApp (CardanoBlock c)) m, MonadIO m) =>
  BlockWithInfo LByteString -> m ()
blocksWriter bwi@BlockWithInfo{biSlotNo, biBlockHeaderHash} = do
  app <- ask
  (blocksToWriteSlotSet, blocksToWriteBlockHashSet) <- readIORef (dsAppWriteBlocks app)
  when (biSlotNo `Set.member` blocksToWriteSlotSet) $ do
    writeBlockWithState bwi Nothing Nothing
    atomicModifyIORef' (dsAppWriteBlocks app) $
      \(slotNoSet, blockHashSet) -> ((Set.delete biSlotNo slotNoSet, blockHashSet), ())
  when (biBlockHeaderHash `Set.member` blocksToWriteBlockHashSet) $ do
    writeBlockWithState bwi Nothing Nothing
    atomicModifyIORef' (dsAppWriteBlocks app) $ \(slotNoSet, blockHashSet) ->
      ((slotNoSet, Set.delete biBlockHeaderHash blockHashSet), ())

diskSnapshotsWriter ::
  ( MonadIO m
  , MonadReader (DbStreamerApp blk) m
  , IsLedger (LedgerState blk)
  ) =>
  ExtLedgerState (CardanoBlock StandardCrypto) mk -> m ()
diskSnapshotsWriter extLedgerState =
  case tipFromExtLedgerState extLedgerState of
    Nothing -> pure ()
    Just tip -> do
      refDiskSnapshots <- dsAppWriteDiskSnapshots <$> ask
      let
        go _ (diskSnapshot : ss)
          | dsNumber diskSnapshot == unSlotNo (tipSlotNo tip) = do
              ledgerDbStoreSnapshot diskSnapshot
              logInfo $ "Created a SnapShot at " <> display (tipSlotNo tip)
              go True ss
          | dsNumber diskSnapshot < unSlotNo (tipSlotNo tip) = do
              ledgerDbStoreSnapshot $ diskSnapshot{dsNumber = unSlotNo (tipSlotNo tip)}
              logWarn $
                "Created a SnapShot at "
                  <> display (tipSlotNo tip)
                  <> " instead of at "
                  <> display (dsNumber diskSnapshot)
                  <> " as requested, since there was no block at that slot number."
              go True ss
        go hasWritten dss =
          when hasWritten $ writeIORef refDiskSnapshots dss
      readIORef refDiskSnapshots >>= go False

writeBlockWithState ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m) =>
  BlockWithInfo LByteString ->
  Maybe (CardanoBlock StandardCrypto) ->
  Maybe (ExtLedgerState (CardanoBlock StandardCrypto) ValuesMK) ->
  m ()
writeBlockWithState BlockWithInfo{biSlotNo, biBlockHeaderHash, biBlockComponent} mBlock mExtLedgerState = do
  mOutDir <- dsAppOutDir <$> ask
  forM_ mOutDir $ \outDir -> do
    let
      rawBlockBytes = biBlockComponent
      blockHeaderHashHex = textDisplay biBlockHeaderHash
      slotNoStr = show (unSlotNo biSlotNo)
      prefix = outDir </> slotNoStr <> "_" <> T.unpack blockHeaderHashHex
      mkTxFileName ix = prefix <> "#" <> show ix <.> "cbor"
      rawBlockFileName = prefix <> "_raw" <.> "cbor"
      blockFileName = prefix <.> "cbor"
    liftIO $ BSL.writeFile rawBlockFileName rawBlockBytes
    logInfo $ "Written raw block to: " <> display (T.pack rawBlockFileName)
    forM_ mBlock $ \block -> do
      withProtocolBlock
        (const $ pure ())
        (\_ -> writeBlock blockFileName . shelleyBlockRaw)
        (\_ -> writeBlock blockFileName . shelleyBlockRaw)
        block
      logInfo $ "Written Block to: " <> display (T.pack blockFileName)
      withBlockTxs
        (liftIO . print)
        (zipWithM_ (\ix -> writeTx (mkTxFileName ix)) [0 :: Int ..])
        block
    forM_ mExtLedgerState $ \extLedgerState -> do
      let epochFileName = outDir </> slotNoStr <> "_newEpochState" <.> "cbor"
      writeNewEpochState epochFileName extLedgerState
      logInfo $ "Written NewEpochState to: " <> display (T.pack epochFileName)
