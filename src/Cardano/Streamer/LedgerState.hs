{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}

module Cardano.Streamer.LedgerState (
  encodeNewEpochState,
  applyNonByronNewEpochState,
  applyNewEpochState,
  lookupRewards,
  lookupTotalRewards,
  writeNewEpochState,
  newEpochStateEpochNo,
  detectNewRewards,
  pattern TickedLedgerStateAllegra,
  pattern TickedLedgerStateAlonzo,
  pattern TickedLedgerStateBabbage,
  pattern TickedLedgerStateConway,
  pattern TickedLedgerStateByron,
  pattern TickedLedgerStateMary,
  pattern TickedLedgerStateShelley,
) where

import Cardano.Chain.Block as Byron (ChainValidationState (cvsUpdateState))
import qualified Cardano.Chain.Slotting as Byron (EpochNumber (getEpochNumber))
import qualified Cardano.Chain.Update.Validation.Interface as Byron (State (currentEpoch))
import Cardano.Ledger.BaseTypes (EpochNo (..))
import Cardano.Ledger.Binary.Plain as Plain (Encoding, ToCBOR, serialize, toCBOR)
import Cardano.Ledger.CertState
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.Shelley.Governance (EraGovernance)
import Cardano.Ledger.Shelley.LedgerState hiding (LedgerState)
import Cardano.Ledger.UMap as UM
import Cardano.Ledger.Val
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map.Merge.Strict as Map
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Protocol.Praos
import Ouroboros.Consensus.Protocol.TPraos
import Ouroboros.Consensus.Shelley.Ledger (shelleyLedgerState)
import Ouroboros.Consensus.Shelley.Ledger.Block
import Ouroboros.Consensus.Shelley.Ledger.Ledger
import RIO
import qualified RIO.Map as Map

import Data.SOP.BasicFunctors
import Data.SOP.Telescope
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HardFork.Combinator.Ledger
import Ouroboros.Consensus.HardFork.Combinator.State.Types as State

encodeNewEpochState :: Crypto c => ExtLedgerState (CardanoBlock c) -> Encoding
encodeNewEpochState = applyNewEpochState toCBOR toCBOR

writeNewEpochState :: (Crypto c, MonadIO m) => FilePath -> ExtLedgerState (CardanoBlock c) -> m ()
writeNewEpochState fp = liftIO . BSL.writeFile fp . Plain.serialize . encodeNewEpochState

pattern TickedLedgerStateByron
  :: TransitionInfo
  -> Ticked (LedgerState ByronBlock)
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateByron ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TZ (State.Current{currentState = Comp st}))
      )

pattern TickedLedgerStateShelley
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (ShelleyEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateShelley ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TZ (State.Current{currentState = Comp st})))
      )

pattern TickedLedgerStateAllegra
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (AllegraEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateAllegra ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))
      )

pattern TickedLedgerStateMary
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (MaryEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateMary ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st})))))
      )

pattern TickedLedgerStateAlonzo
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (AlonzoEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateAlonzo ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))))
      )

pattern TickedLedgerStateBabbage
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (Praos c) (BabbageEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateBabbage ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st})))))))
      )

pattern TickedLedgerStateConway
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (Praos c) (ConwayEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateConway ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))))))
      )

{-# COMPLETE
  TickedLedgerStateByron
  , TickedLedgerStateShelley
  , TickedLedgerStateAllegra
  , TickedLedgerStateMary
  , TickedLedgerStateAlonzo
  , TickedLedgerStateBabbage
  , TickedLedgerStateConway
  #-}

applyNewEpochState
  :: Crypto c
  => (ChainValidationState -> a)
  -> ( forall era
        . ( EraCrypto era ~ c
          , EraTxOut era
          , EraGovernance era
          , ToCBOR (NewEpochState era)
          )
       => NewEpochState era
       -> a
     )
  -> ExtLedgerState (CardanoBlock c)
  -> a
applyNewEpochState fByronBased fShelleyBased extLedgerState =
  case ledgerState extLedgerState of
    LedgerStateByron ls -> fByronBased (byronLedgerState ls)
    LedgerStateShelley ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateAllegra ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateMary ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateAlonzo ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateBabbage ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateConway ls -> fShelleyBased (shelleyLedgerState ls)

applyNonByronNewEpochState
  :: Crypto c
  => ( forall era
        . ( EraCrypto era ~ c
          , EraTxOut era
          , EraGovernance era
          , ToCBOR (NewEpochState era)
          )
       => NewEpochState era
       -> a
     )
  -> ExtLedgerState (CardanoBlock c)
  -> Maybe a
applyNonByronNewEpochState f = applyNewEpochState (const Nothing) (Just . f)

applyTickedNewEpochState
  :: Crypto c
  => (TransitionInfo -> ChainValidationState -> a)
  -> ( forall era
        . ( EraCrypto era ~ c
          , EraTxOut era
          , EraGovernance era
          , ToCBOR (NewEpochState era)
          )
       => TransitionInfo
       -> NewEpochState era
       -> a
     )
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> a
applyTickedNewEpochState fByronBased fShelleyBased tickedExtLedgerState =
  case tickedLedgerState tickedExtLedgerState of
    TickedLedgerStateByron ti ls -> fByronBased ti (tickedByronLedgerState ls)
    TickedLedgerStateShelley ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateAllegra ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateMary ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateAlonzo ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateBabbage ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateConway ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)

applyTickedNonByronNewEpochState
  :: Crypto c
  => ( forall era
        . ( EraCrypto era ~ c
          , EraTxOut era
          , EraGovernance era
          , ToCBOR (NewEpochState era)
          )
       => TransitionInfo
       -> NewEpochState era
       -> a
     )
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> Maybe a
applyTickedNonByronNewEpochState f =
  applyTickedNewEpochState (\ _ _ -> Nothing) (\ti -> Just . f ti)

lookupStakeCredentials
  :: Set (Credential 'Staking (EraCrypto era))
  -> NewEpochState era
  -> UM.StakeCredentials (EraCrypto era)
lookupStakeCredentials creds nes =
  let um = dsUnified (certDState (lsCertState (esLState (nesEs nes))))
   in UM.domRestrictedStakeCredentials creds um

lookupRewards
  :: Set (Credential 'Staking (EraCrypto era))
  -> NewEpochState era
  -> Map (Credential 'Staking (EraCrypto era)) Coin
lookupRewards creds nes = scRewards $ lookupStakeCredentials creds nes

lookupTotalRewards :: Set (Credential 'Staking (EraCrypto era)) -> NewEpochState era -> Maybe Coin
lookupTotalRewards creds nes = guard (Map.null credsRewards) >> pure (fold credsRewards)
  where
    credsRewards = lookupRewards creds nes

newEpochStateEpochNo :: Crypto c => ExtLedgerState (CardanoBlock c) -> EpochNo
newEpochStateEpochNo =
  applyNewEpochState
    (EpochNo . Byron.getEpochNumber . Byron.currentEpoch . Byron.cvsUpdateState)
    nesEL

detectNewRewards
  :: Crypto c
  => Set (Credential 'Staking c)
  -> EpochNo
  -> Map (Credential 'Staking c) Coin
  -> Map (Credential 'Staking c) Coin
  -> ExtLedgerState (CardanoBlock c)
  -> Maybe (EpochNo, Map (Credential 'Staking c) Coin, Map (Credential 'Staking c) Coin)
detectNewRewards creds prevEpochNo prevRewards epochWithdrawals extLedgerState = do
  let curEpochNo = newEpochStateEpochNo extLedgerState
  unless (curEpochNo == prevEpochNo || curEpochNo == prevEpochNo + 1) $
    error $
      "Current epoch number: "
        ++ show curEpochNo
        ++ " is too far apart from the previous one: "
        ++ show prevEpochNo
  -- rewards are payed out on the epoch boundary
  guard (curEpochNo == prevEpochNo + 1)
  curRewards <- applyNonByronNewEpochState (lookupRewards creds) extLedgerState
  let epochReceivedRewards =
        Map.merge
          Map.dropMissing -- credential was unregistered
          (Map.filterMissing $ \_ c -> c /= mempty) -- credential was registered. Discard zero rewards
          ( Map.zipWithMaybeMatched $ \cred cp cc -> do
              -- Need to adjust for withdrawals
              let cpw = maybe cp (cp <->) $ Map.lookup cred epochWithdrawals
              guard (cpw /= cc) -- no change, means no new rewards
              when (cc < cpw) $ error "New reward amounts can't be smaller than the previous ones"
              let newRewardAmount = cc <-> cpw
              guard (newRewardAmount /= mempty) -- Discard zero change to rewards
              Just newRewardAmount -- new reward amount
          )
          prevRewards
          curRewards
  pure (curEpochNo, curRewards, epochReceivedRewards)
