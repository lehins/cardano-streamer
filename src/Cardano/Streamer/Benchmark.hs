{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.Benchmark where

import Cardano.Ledger.Crypto
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState (tickedExtLedgerStateEpochNo)

-- import Cardano.Streamer.Producer
import Conduit
import Criterion.Measurement (getCPUTime, getCycles, getTime, initializeTime)
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HardFork.Combinator.State.Types
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerState)
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified RIO.Text as T

data Measure = Measure
  { measureTime :: !Double
  , measureCPUTime :: !Double
  , measureCycles :: !Word64
  }
  deriving (Eq, Show)

instance Display Measure where
  display Measure{..} =
    mconcat
      [ display measureTime
      , " "
      , display measureCPUTime
      , " "
      , display measureCycles
      ]

divMeasure :: Measure -> Word64 -> Measure
divMeasure m d =
  Measure
    { measureTime = measureTime m / fromIntegral d
    , measureCPUTime = measureCPUTime m / fromIntegral d
    , measureCycles = measureCycles m `div` d
    }

instance Semigroup Measure where
  (<>) m1 m2 =
    Measure
      { measureTime = measureTime m1 + measureTime m2
      , measureCPUTime = measureCPUTime m1 + measureCPUTime m2
      , measureCycles = measureCycles m1 + measureCycles m2
      }

instance Monoid Measure where
  mempty =
    Measure
      { measureTime = 0
      , measureCPUTime = 0
      , measureCycles = 0
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

measureAction_ :: MonadIO m => m a -> m Measure
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

-- replayWithBenchmarking
--   :: ExtLedgerState (CardanoBlock StandardCrypto)
--   -> ConduitT
--       a
--       Stat
--       (RIO (DbStreamerApp (CardanoBlock StandardCrypto)))
--       (ExtLedgerState (CardanoBlock StandardCrypto))
withBenchmarking f = do
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
  liftIO initializeTime
  f benchRunTick benchRunBlock

-- runWithBench = advanceBlockGranular benchRunTick benchRunBlock
-- sourceBlocksWithState GetBlock initLedgerState runWithBench

data MeasureSummary = MeasureSummary
  { msMean :: !Measure
  , -- msMedean :: !Measure -- not possible to compute in streaming fashion
    -- TODO: implement median estimation algorithm
    -- ,
    msSum :: !Measure
  , msCount :: !Word64
  }

instance Display MeasureSummary where
  display = displayMeasure 0

displayMeasure :: Int -> MeasureSummary -> Utf8Builder
displayMeasure n MeasureSummary{..} =
  mconcat
    [ prefix <> "Mean: " <> display msMean
    , prefix <> "Sum: " <> display msSum
    , prefix <> "Total: " <> display msCount
    ]
  where
    prefix = "\n" <> display (T.replicate n " ")

data StatsReport = StatsReport
  { srTick :: !MeasureSummary
  , srEpoch :: !MeasureSummary
  , srBlock :: !MeasureSummary
  }

instance Display StatsReport where
  display StatsReport{..} =
    mconcat
      [ "Ticks:  "
      , display srTick
      , "\nEpochs: "
      , display srEpoch
      , "\nBlocks: "
      , display srBlock
      ]

calcStatsReport :: Monad m => ConduitT Stat Void m StatsReport
calcStatsReport = do
  (tick, epoch, block) <- foldlC addMeasure ((mempty, 0), (mempty, 0), (mempty, 0))
  pure $
    StatsReport
      { srTick = uncurry mkMeasureSummary tick
      , srEpoch = uncurry mkMeasureSummary epoch
      , srBlock = uncurry mkMeasureSummary block
      }
  where
    addMeasure
      ( tick@(!tickSum, !tickCount)
        , epoch@(!epochSum, !epochCount)
        , (!blockSum, !blockCount)
        )
      stat =
        let !(!tick', !epoch') =
              case tickEpochNo (tickStat stat) of
                Nothing -> ((tickSum <> tickMeasure (tickStat stat), tickCount + 1), epoch)
                Just _ -> (tick, (epochSum <> tickMeasure (tickStat stat), epochCount + 1))
            !block' = (blockSum <> blockMeasure (blockStat stat), blockCount + 1)
         in (tick', epoch', block')
    mkMeasureSummary mSum mCount =
      MeasureSummary
        { msMean = mSum `divMeasure` mCount
        , msSum = mSum
        , msCount = mCount
        }
