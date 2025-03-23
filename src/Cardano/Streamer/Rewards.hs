{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.Rewards (
  RewardDistribution (..),
  writeRewardsHeaders,
  writeRewardDistribution,
)
where

import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Coin
import Cardano.Streamer.Common
import Data.ByteString.Builder (lazyByteString)
import Data.Csv
import Data.Csv.Incremental as CSV
import RIO.Time

data RewardDistribution
  = RewardDistribution
  { rdEra :: !CardanoEra
  -- ^ Era in which rewards where distributed
  , rdEpochNo :: !EpochNo
  -- ^ Beginning of an epoch in which rewards where distributed
  , rdSlotNo :: !SlotNo
  -- ^ Slot number in which rewards where distributed
  , rdUTCTime :: !UTCTime
  -- ^ Approximate time when the rewards where distributed
  , rdRewardAmount :: !Coin
  -- ^ Amount of the reward that was distributed
  }
  deriving (Show, Generic)

instance ToRecord RewardDistribution

writeRewardsHeaders :: (MonadReader (DbStreamerApp blk) m, MonadIO m) => m ()
writeRewardsHeaders = do
  handles <- dsAppRewardsHandles <$> ask
  forM_ handles $ \hdl -> do
    let rewardsHeader =
          [ "era"
          , "epoch_number"
          , "slot_number"
          , "time"
          , "reward_account"
          , "reward_amount"
          ]
    hPutBuilder hdl $ lazyByteString $ CSV.encode $ CSV.encodeRecord $ record rewardsHeader

writeRewardDistribution ::
  MonadIO m =>
  Handle ->
  RewardDistribution ->
  m ()
writeRewardDistribution hdl rd = do
  hPutBuilder hdl $ lazyByteString $ CSV.encode $ CSV.encodeRecord rd
