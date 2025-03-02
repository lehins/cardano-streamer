{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Cardano.Streamer.Measure (
  Measure (..),
  measureAction,
  measureAction_,
  divMeasure,
  MeasureSummary (..),
  maxDepthMeasureSummary,
  displayMeasureSummary,
) where

import Cardano.Streamer.Time
import Conduit
import Criterion.Measurement (getCPUTime, getCycles, getTime)
import Data.Aeson
import Data.Fixed
import RIO
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

measureAction :: MonadIO m => m a -> m (Measure, a)
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
  pure (mes, res)

measureAction_ :: MonadIO m => m a -> m Measure
measureAction_ action = fst <$> measureAction action

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

doubleToDiffTime :: Double -> NominalDiffTime
doubleToDiffTime d = secondsToNominalDiffTime f
  where
    f = MkFixed $ round (d * fromIntegral (resolution f))
