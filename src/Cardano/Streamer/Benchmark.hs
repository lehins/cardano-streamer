{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Cardano.Streamer.Benchmark where

import Cardano.Ledger.Crypto
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState (extLedgerStateEpochNo, tickedExtLedgerStateEpochNo)
import Cardano.Streamer.Time
import Conduit
import Criterion.Measurement (getCPUTime, getCycles, getTime, initializeTime)
import Data.Aeson
import Data.Fixed
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerState)
import Ouroboros.Consensus.Ticked (Ticked)
import qualified RIO.Text as T
import RIO.Time (NominalDiffTime, secondsToNominalDiffTime)
import Text.Printf

data Measure = Measure
  { measureTime :: !Double
  , measureCPUTime :: !Double
  , measureCycles :: !Word64
  }
  deriving (Eq, Show, Generic)

instance ToJSON Measure where
  toJSON = genericToJSON (defaultOptions{fieldLabelModifier = drop 2})

instance Display Measure where
  display = displayMeasure Nothing

maxDepthMeasure :: Measure -> Int
maxDepthMeasure Measure{..} =
  max
    (maxTimeDepth $ diffTimeToMicro $ doubleToDiffTime measureTime)
    (maxTimeDepth $ diffTimeToMicro $ doubleToDiffTime measureCPUTime)

divMeasure :: Measure -> Word64 -> Measure
divMeasure m d
  | d == 0 = m
  | otherwise =
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
  , blockApplyMeasure :: {-# UNPACK #-} !Measure
  , blockDecodeMeasure :: {-# UNPACK #-} !Measure
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

measureAction :: MonadIO m => m a -> m (a, Measure)
measureAction action = do
  startTime <- liftIO getTime
  startCPUTime <- liftIO getCPUTime
  startCycles <- liftIO getCycles
  !res <- action
  endTime <- liftIO getTime
  endCPUTime <- liftIO getCPUTime
  endCycles <- liftIO getCycles
  let !mes =
        Measure
          { measureTime = endTime - startTime
          , measureCPUTime = endCPUTime - startCPUTime
          , measureCycles = endCycles - startCycles
          }
  pure (res, mes)

measureAction_ :: MonadIO m => m a -> m Measure
measureAction_ action = snd <$> measureAction action

withBenchmarking
  :: forall c m mc a b
   . (Crypto c, MonadIO mc, MonadIO m)
  => ( (m (CardanoBlock c) -> m (CardanoBlock c, Measure))
       -> ( ExtLedgerState (CardanoBlock c)
            -> Ticked (ExtLedgerState (CardanoBlock c))
            -> SlotNo
            -> m TickStat
          )
       -> (m a -> (Measure, TickStat) -> m (a, Stat))
       -> mc b
     )
  -> mc b
withBenchmarking f = do
  let
    benchDecode decodeBlock = measureAction decodeBlock
    benchRunTick
      :: ExtLedgerState (CardanoBlock c)
      -> Ticked (ExtLedgerState (CardanoBlock c))
      -> SlotNo
      -> m TickStat
    benchRunTick prevLedgerState tickedLedgerState slotNo = do
      measure <- measureAction_ (pure tickedLedgerState)
      let prevEpochNo = extLedgerStateEpochNo prevLedgerState
          (_ti, newEpochNo) = tickedExtLedgerStateEpochNo tickedLedgerState
      pure $!
        if prevEpochNo /= newEpochNo
          then TickStat slotNo (Just newEpochNo) measure
          else TickStat slotNo Nothing measure
    benchRunBlock getExtLedgerState (decodeMeasure, tickStat) = do
      (extLedgerState, applyMeasure) <- measureAction getExtLedgerState
      let !blockStat = BlockStat 0 0 applyMeasure decodeMeasure
          !stat = Stat tickStat blockStat
      pure (extLedgerState, stat)
  liftIO initializeTime
  f benchDecode benchRunTick benchRunBlock

data MeasureSummary = MeasureSummary
  { msMean :: !Measure
  , -- msMedean :: !Measure -- not possible to compute in streaming fashion
    -- TODO: implement median estimation algorithm
    -- ,
    msSum :: !Measure
  , msCount :: !Word64
  }
  deriving (Generic)

instance ToJSON MeasureSummary where
  toJSON = genericToJSON (defaultOptions{fieldLabelModifier = drop 2})

instance Display MeasureSummary where
  display ms = displayMeasureSummary (maxDepthMeasureSummary ms) 0 ms

maxDepthMeasureSummary :: MeasureSummary -> Int
maxDepthMeasureSummary MeasureSummary{..} =
  max (maxDepthMeasure msMean) (maxDepthMeasure msSum)

displayMeasureSummary :: Int -> Int -> MeasureSummary -> Utf8Builder
displayMeasureSummary depth n MeasureSummary{..} =
  mconcat
    [ prefix <> "Mean:  " <> displayMeasure (Just depth) msMean
    , prefix <> "Sum:   " <> displayMeasure (Just depth) msSum
    , prefix <> "Count: " <> display msCount
    ]
  where
    prefix = "\n" <> display (T.replicate n " ")

displayMeasure :: Maybe Int -> Measure -> Utf8Builder
displayMeasure mDepth Measure{..} =
  mconcat
    [ "Time:["
    , displayDoubleMeasure measureTime
    , "] CPUTime:["
    , displayDoubleMeasure measureCPUTime
    , "] Cycles:["
    , display . T.pack $ showMeasureCycles measureCycles
    , "]"
    ]
  where
    showMeasureCycles = if isJust mDepth then printf "%20d" else show
    displayDoubleMeasure =
      display . T.pack . showTime mDepth True . diffTimeToMicro . doubleToDiffTime

data StatsReport = StatsReport
  { srTick :: !MeasureSummary
  , srEpoch :: !MeasureSummary
  , srBlock :: !MeasureSummary
  , srBlockDecode :: !MeasureSummary
  }
  deriving (Generic)

instance ToJSON StatsReport where
  toJSON = genericToJSON (defaultOptions{fieldLabelModifier = drop 2})

maxDepthStatsReport :: StatsReport -> Int
maxDepthStatsReport StatsReport{..} =
  maximum
    [ maxDepthMeasureSummary srTick
    , maxDepthMeasureSummary srEpoch
    , maxDepthMeasureSummary srBlock
    , maxDepthMeasureSummary srBlockDecode
    ]

instance Display StatsReport where
  display sr@StatsReport{..} =
    let n = 5
        title str = "\n== " <> str <> ": ========"
        maxDepth = maxDepthStatsReport sr
     in mconcat
          [ title "Ticks"
          , displayMeasureSummary maxDepth n srTick
          , title "Epochs"
          , displayMeasureSummary maxDepth n srEpoch
          , title "Apply Blocks"
          , displayMeasureSummary maxDepth n srBlock
          , title "Decode Blocks"
          , displayMeasureSummary maxDepth n srBlockDecode
          ]

calcStatsReport :: Monad m => ConduitT Stat Void m StatsReport
calcStatsReport = do
  (tick, epoch, (applyBlock, decodeBlock, blockCount)) <-
    foldlC addMeasure ((mempty, 0), (mempty, 0), (mempty, mempty, 0))
  pure $
    StatsReport
      { srTick = uncurry mkMeasureSummary tick
      , srEpoch = uncurry mkMeasureSummary epoch
      , srBlock = mkMeasureSummary applyBlock blockCount
      , srBlockDecode = mkMeasureSummary decodeBlock blockCount
      }
  where
    addMeasure
      ( !tick@(!tickSum, !tickCount)
        , !epoch@(!epochSum, !epochCount)
        , !(!blockSum, !blockDecodeSum, !blockCount)
        )
      stat =
        let !(!tick', !epoch') =
              case tickEpochNo (tickStat stat) of
                Nothing -> ((tickSum <> tickMeasure (tickStat stat), tickCount + 1), epoch)
                Just _ -> (tick, (epochSum <> tickMeasure (tickStat stat), epochCount + 1))
            !block' =
              ( blockSum <> blockApplyMeasure (blockStat stat)
              , blockDecodeSum <> blockDecodeMeasure (blockStat stat)
              , blockCount + 1
              )
         in (tick', epoch', block')
    mkMeasureSummary mSum mCount =
      MeasureSummary
        { msMean = mSum `divMeasure` mCount
        , msSum = mSum
        , msCount = mCount
        }

doubleToDiffTime :: Double -> NominalDiffTime
doubleToDiffTime d = secondsToNominalDiffTime f
  where
    f = MkFixed $ round (d * fromIntegral (resolution f))
