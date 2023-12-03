{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilyDependencies #-}

module Cardano.Streamer.Time where

import Data.Fixed
import Data.Maybe
import Data.Word
import RIO.Time
import Text.Printf

diffTimeToMicro :: NominalDiffTime -> Time 'Micro
diffTimeToMicro ndt =
  case nominalDiffTimeToSeconds ndt of
    MkFixed pico -> addTime (Time 0 Nothing) (pico `div` 1000000)

data T
  = Year
  | Day
  | Hour
  | Min
  | Sec
  | Milli
  | Micro
  | Nano
  | Pico

type family SubTime t = nt | nt -> t where
  SubTime 'Pico = 'Nano
  SubTime 'Nano = 'Micro
  SubTime 'Micro = 'Milli
  SubTime 'Milli = 'Sec
  SubTime 'Sec = 'Min
  SubTime 'Min = 'Hour
  SubTime 'Hour = 'Day
  SubTime 'Day = 'Year

data Time (t :: T) where
  Time :: !Word64 -> !(Maybe (Time (SubTime t))) -> Time t

instance Interval t => Show (Time t) where
  show = showTime Nothing True

class Interval t => BoundedInterval (t :: T) where
  maxInterval :: proxy t -> Integer

maxIntervalDigits :: BoundedInterval t => proxy t -> Int
maxIntervalDigits px = length $ show (maxInterval px - 1)

alignTimes :: Time t -> Time t -> (Time t, Time t)
alignTimes t1@(Time _ Nothing) t2@(Time _ Nothing) = (t1, t2)
alignTimes (Time i1 (Just si1)) (Time i2 Nothing) =
  case alignTimes si1 (Time 0 Nothing) of
    (_, si2) -> (Time i1 (Just si1), Time i2 (Just si2))
alignTimes (Time i1 Nothing) (Time i2 (Just si2)) =
  case alignTimes (Time 0 Nothing) si2 of
    (si1, _) -> (Time i1 (Just si1), Time i2 (Just si2))
alignTimes (Time i1 (Just si1)) (Time i2 (Just si2)) =
  case alignTimes si1 si2 of
    (si1', si2') -> (Time i1 (Just si1'), Time i2 (Just si2'))

class Interval (t :: T) where
  timeName :: proxy t -> String
  timeNameShort :: proxy t -> String

  showTime :: Maybe Int -> Bool -> Time t -> String
  -- default showTime
  --   :: (BoundedInterval t, Interval (SubTime t))
  --   => Maybe Int
  --   -> Bool
  --   -> Time t
  --   -> String
  -- showTime = defaultShowTime

  addTime :: Time t -> Integer -> Time t
  -- default addTime :: (BoundedInterval t, Interval (SubTime t)) => Time t -> Integer -> Time t
  -- addTime = defaultAddTime

maxTimeDepth :: Time t -> Int
maxTimeDepth (Time _ Nothing) = 1
maxTimeDepth (Time _ (Just t)) = 1 + maxTimeDepth t

defaultShowTime
  :: forall t
   . (BoundedInterval t, Interval (SubTime t))
  => Maybe Int
  -> Bool
  -> Time t
  -> String
defaultShowTime mDepth isConcise t@(Time i mSub) =
  case mSub of
    Nothing
      | Just depth <- mDepth
      , depth > 1 ->
          let zeroTime = showTime (Just (depth - 1)) isConcise (Time 0 Nothing :: Time (SubTime t))
           in replicate (length zeroTime) ' ' <> sep <> v
      | otherwise -> v
    Just sub -> showTime (pred <$> mDepth) isConcise sub <> sep <> v
  where
    v =
      printf ("%0" ++ show (maxIntervalDigits t) ++ "d") i
        <> if isConcise then timeNameShort t else " " <> timeName t
    sep = if isConcise then " " else ", "

defaultAddTime :: (BoundedInterval t, Interval (SubTime t)) => Time t -> Integer -> Time t
defaultAddTime t@(Time p maybeNext) i =
  case (toInteger p + i) `quotRem` maxInterval t of
    (0, r) -> Time (fromInteger r) maybeNext
    (q, r) -> Time (fromInteger r) $ Just (addTime (fromMaybe (Time 0 Nothing) maybeNext) q)

instance BoundedInterval 'Pico where
  maxInterval _ = 1000
instance Interval 'Pico where
  timeName _ = "picosecond"
  timeNameShort _ = "ps"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Nano where
  maxInterval _ = 1000
instance Interval 'Nano where
  timeName _ = "nanosecond"
  timeNameShort _ = "ns"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Micro where
  maxInterval _ = 1000
instance Interval 'Micro where
  timeName _ = "microsecond"
  timeNameShort _ = "μs"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Milli where
  maxInterval _ = 1000
instance Interval 'Milli where
  timeName _ = "millisecond"
  timeNameShort _ = "ms"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Sec where
  maxInterval _ = 60
instance Interval 'Sec where
  timeName _ = "second"
  timeNameShort _ = "s"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Min where
  maxInterval _ = 60
instance Interval 'Min where
  timeName _ = "minute"
  timeNameShort _ = "m"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Hour where
  maxInterval _ = 24
instance Interval 'Hour where
  timeName _ = "hour"
  timeNameShort _ = "h"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance BoundedInterval 'Day where
  maxInterval _ = 365
instance Interval 'Day where
  timeName _ = "day"
  timeNameShort _ = "d"
  showTime = defaultShowTime
  addTime = defaultAddTime

instance Interval 'Year where
  timeName _ = "year"
  timeNameShort _ = "y"
  showTime _ isConcise t@(Time i _) =
    show i <> if isConcise then timeNameShort t else " " <> timeName t
  addTime (Time y n) i = Time (y + fromInteger i) n
