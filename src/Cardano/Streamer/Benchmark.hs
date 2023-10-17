{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.Benchmark where

import Criterion.Measurement
import Ouroboros.Consensus.HardFork.Combinator.State.Types
import Cardano.Crypto.Hash.Class (hashToTextAsHex)
import Cardano.Ledger.Coin
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.SafeHash (extractHash)
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState (tickedExtLedgerStateEpochNo,
                                     detectNewRewards, extLedgerStateEpochNo, writeNewEpochState)
import Cardano.Streamer.Producer
import Cardano.Streamer.ProtocolInfo
import Conduit
import Control.Monad.Trans.Except
import Data.Foldable
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Merge.Strict as Map
import qualified Data.Map.Strict as Map
import Data.Monoid
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HeaderValidation (
  AnnTip,
  HasAnnTip (..),
  annTipPoint,
  headerStateTip,
 )
import Ouroboros.Consensus.Ledger.Abstract (
  applyBlockLedgerResult,
  reapplyBlockLedgerResult,
  tickThenApplyLedgerResult,
  tickThenReapplyLedgerResult,
 )
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (lrResult), applyChainTickLedgerResult)
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerCfg (..), ExtLedgerState (headerState), Ticked)
import Ouroboros.Consensus.Ledger.SupportsProtocol (LedgerSupportsProtocol)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Util.ResourceRegistry
import RIO.FilePath
import qualified RIO.Set as Set
import qualified RIO.Text as T

data Measure = Measure
  { measureTime :: !Double
  , measureCPUTime :: !Double
  , measureCycles :: !Word64
  }

data BlockStat = BlockStat
  { blockNumTxs :: !Int
  , blockNumScripts :: !Int
  , blockMeasure :: {-# UNPACK #-} !Measure
  }

data TickStat = TickStat
  { tickSlotNo :: !SlotNo
  , tickEpochNo :: !(Maybe EpochNo)
  , tickMeasure :: {-# UNPACK #-} !Measure
  }

data Stat = Stat
  { tickStat :: {-# UNPACK #-} !TickStat
  , blockStat :: {-# UNPACK #-} !BlockStat
  }

measureAction_ action = do
  startTime <- liftIO getTime
  startCPUTime <- liftIO getCPUTime
  startCycles <- liftIO getCycles
  !_ <- action
  endTime <- liftIO getTime
  endCPUTime <- liftIO getCPUTime
  endCycles <- liftIO getCycles
  pure $!
    Measure
      { measureTime = endTime - startTime
      , measureCPUTime = endCPUTime - startCPUTime
      , measureCycles = endCycles - startCycles
      }

replayWithBenchmarking initLedgerState = do
  liftIO initializeTime
  let
    benchRunTick tickedLedgerState slotNo = do
      measure <- measureAction_ (pure tickedLedgerState)
      pure $! case tickedExtLedgerStateEpochNo tickedLedgerState of
        (TransitionKnown epochNo, _) ->
          TickStat slotNo (Just epochNo) measure
        _ -> TickStat slotNo Nothing measure
    benchRunBlock _ extLedgerState tickStat = do
      measure <- measureAction_ (pure extLedgerState)
      let blockStat = BlockStat 0 0 measure
      pure $! Stat tickStat blockStat
    runWithBench = advanceBlockGranular benchRunTick benchRunBlock
  runConduit $ void (sourceBlocksWithState GetBlock initLedgerState runWithBench) .| sinkList
