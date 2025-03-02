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

import Ouroboros.Consensus.Ledger.Basics (ComputeLedgerEvents(..), LedgerResult(..))
import Cardano.Streamer.Common
import Cardano.Streamer.Inspection
import Cardano.Streamer.Measure
import Conduit
import Data.Aeson
import Ouroboros.Consensus.Block

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

benchmarkInspection :: SlotInspection b Measure () Measure Measure () Stat
benchmarkInspection =
  SlotInspection
    { siDecodeBlock = \_ -> measureAction
    , siLedgerDbLookup = const unitPrefix
    , siTick = \_ action -> measureAction (lrResult <$> action OmitLedgerEvents)
    , siApplyBlock = \_ action -> measureAction (action OmitLedgerEvents)
    , siLedgerDbPush = \_ -> id
    , siFinal =
        \swb decodeMeasure _ tickMeasure applyBlockMeasure _ ->
          let
            !slotNo = swbSlotNo swb
            !tickStat =
              if isFirstSlotOfNewEpoch swb
                then TickStat slotNo (Just (swbEpochNo swb)) tickMeasure
                else TickStat slotNo Nothing tickMeasure
            !blockStat = BlockStat 0 0 applyBlockMeasure decodeMeasure
           in
            pure $! Stat tickStat blockStat
    }

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
