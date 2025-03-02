{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.Rewards (
  RewardDistribution (..),
  rewardsInspection,
  writeRewardsHeaders,
  writeRewardDistribution,
)
where

import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Slotting.EpochInfo (epochInfoSlotToUTCTime)
import Cardano.Streamer.Common
import Cardano.Streamer.Inspection
import Cardano.Streamer.Ledger
import Cardano.Streamer.LedgerState
import Data.ByteString.Builder (lazyByteString)
import Data.Csv
import Data.Csv.Incremental as CSV
import Ouroboros.Consensus.Ledger.Abstract (ComputeLedgerEvents (..))
import Ouroboros.Consensus.Ledger.Basics (LedgerResult (..))
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Ouroboros.Consensus.Shelley.Ledger.Ledger (ShelleyLedgerEvent (..))
import RIO.Time
import qualified Data.Map.Strict as Map

data RewardDistribution a
  = RewardDistribution
  { rdEra :: !CardanoEra
  -- ^ Era in which rewards where distributed
  , rdEpochNo :: !EpochNo
  -- ^ Beginning of an epoch in which rewards where distributed
  , rdSlotNo :: !SlotNo
  -- ^ Slot number in which rewards where distributed
  , rdUTCTime :: !UTCTime
  -- ^ Approximate time when the rewards where distributed
  , rdRewards :: !a
  -- ^ Amount of the reward that was distributed
  }
  deriving (Show, Generic)

instance ToField a => ToRecord (RewardDistribution a)

writeRewardsHeaders ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m, Foldable f) => f Handle -> m ()
writeRewardsHeaders rewardHandles = do
  forM_ rewardHandles $ \hdl -> do
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
  RewardDistribution Coin ->
  m ()
writeRewardDistribution hdl rd = do
  hPutBuilder hdl $ lazyByteString $ CSV.encode $ CSV.encodeRecord rd

rewardsInspection ::
  SlotInspection
    b
    ()
    ()
    [Map (Credential Staking) Coin]
    ()
    ()
    (Maybe (RewardDistribution (Map (Credential Staking) Coin)))
rewardsInspection =
  SlotInspection
    { siDecodeBlock = const unitPrefix
    , siLedgerDbLookup = const unitPrefix
    , siTick = \_ action -> do
        result <- action ComputeLedgerEvents
        let rewards =
              extractLedgerEvents (lrEvents result) $ \event ->
                case event of
                  ShelleyLedgerEventBBODY _ -> Nothing
                  ShelleyLedgerEventTICK tickEvent -> Just (getRewardsFromEvents event tickEvent)
        pure (rewards, lrResult result)
    , siApplyBlock = \_ action -> unitPrefix $ action OmitLedgerEvents
    , siLedgerDbPush = \_ -> id
    , siFinal = \swb _ _ rewards _ _ -> do
        app <- ask
        -- There are no rewards distributions on ticks that aren't at least in Shelley era and don't
        -- cross eboch boundary
        let epochCrossingGlobals = do
              guard (isFirstSlotOfNewEpoch swb)
              swbGlobals swb (pInfoConfig $ dsAppProtocolInfo app)
            slotNo = swbSlotNo swb
        forM epochCrossingGlobals $ \globals -> do
          utcTime <-
            case epochInfoSlotToUTCTime (epochInfo globals) (systemStart globals) slotNo of
              Left err ->
                throwString $
                  "Could not convert slot " <> show slotNo <> " to UTCTime: " <> show err
              Right utcTime -> pure utcTime
          pure $
            RewardDistribution
              { rdEra = swbCardanoEra swb
              , rdEpochNo = swbEpochNo swb
              , rdSlotNo = slotNo
              , rdUTCTime = utcTime
              , rdRewards =
                  -- There should only ever be at most one entry in the list anyways
                  Map.unionsWith (<>) rewards
              }
    }
