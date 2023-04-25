{-# LANGUAGE LambdaCase #-}

module Cardano.Streamer.Producer where

import Ouroboros.Consensus.Ledger.SupportsProtocol (LedgerSupportsProtocol)
import Cardano.Streamer.Common
import Conduit
import Control.Monad.Trans.Except
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.HeaderValidation (
  AnnTip,
  HasAnnTip (..),
  annTipPoint,
  headerStateTip,
 )
import Ouroboros.Consensus.Ledger.Abstract (tickThenApplyLedgerResult)
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (lrResult))
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState))
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB

sourceBlocks
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasHeader blk
     , HasAnnTip blk
     )
  => ImmutableDB.ImmutableDB IO blk
  -> BlockComponent blk b
  -> WithOrigin (AnnTip blk)
  -> ConduitT i b m ()
sourceBlocks iDb blockComponent withOriginAnnTip = do
  registry <- view registryL
  itr <- liftIO $ case withOriginAnnTip of
    Origin ->
      ImmutableDB.streamAll iDb registry blockComponent
    NotOrigin annTip ->
      ImmutableDB.streamAfterKnownPoint iDb registry blockComponent (annTipPoint annTip)
  fix $ \loop ->
    liftIO (ImmutableDB.iteratorNext itr) >>= \case
      ImmutableDB.IteratorExhausted -> pure ()
      ImmutableDB.IteratorResult b -> yield b >> loop

foldBlocksWithState
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasHeader blk
     , HasAnnTip blk
     )
  => ImmutableDB.ImmutableDB IO blk
  -> BlockComponent blk b
  -> ExtLedgerState blk
  -- ^ Initial ledger state
  -> (ExtLedgerState blk -> b -> m (ExtLedgerState blk))
  -- ^ Function to process each block with current ledger state
  -> ConduitT a c m (ExtLedgerState blk)
foldBlocksWithState iDb blockComponent initState action =
  let withOriginAnnTip = headerStateTip (headerState initState)
   in sourceBlocks iDb blockComponent withOriginAnnTip .| foldMC action initState

sourceBlocksWithState
  :: ( MonadIO m
     , MonadReader env m
     , HasResourceRegistry env
     , HasHeader blk
     , HasAnnTip blk
     )
  => ImmutableDB.ImmutableDB IO blk
  -> BlockComponent blk b
  -> ExtLedgerState blk
  -> (ExtLedgerState blk -> b -> m (ExtLedgerState blk, c))
  -> ConduitT a c m (ExtLedgerState blk)
sourceBlocksWithState iDb blockComponent initState action =
  sourceBlocks iDb blockComponent withOriginAnnTip .| go initState
  where
    withOriginAnnTip = headerStateTip (headerState initState)
    go ledgerState =
      await >>= \case
        Nothing -> pure ledgerState
        Just b -> do
          (ledgerState', c) <- lift $ action ledgerState b
          yield c
          go ledgerState'

validateLedger
  :: LedgerSupportsProtocol blk
  => ExtLedgerState blk
  -> blk
  -> RIO (DbStreamerApp blk) (ExtLedgerState blk)
validateLedger prevLedger block = do
  ledgerCfg <- ExtLedgerCfg . pInfoConfig . dsAppProtocolInfo <$> ask
  either (throwString . show) (pure . lrResult) $
    runExcept $
      tickThenApplyLedgerResult ledgerCfg block prevLedger
