{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module Cardano.Streamer.LedgerState (
  encodeNewEpochState,
  applyNonByronNewEpochState,
  applyNewEpochState,
  lookupReward,
  writeNewEpochState,
) where

import Cardano.Chain.Block (ChainValidationState)
import Cardano.Ledger.Binary.Plain as Plain (Encoding, ToCBOR, serialize, toCBOR)
import Cardano.Ledger.CertState
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.Shelley.Governance (EraGovernance)
import Cardano.Ledger.Shelley.LedgerState
import Cardano.Ledger.UMap as UM
import qualified Data.ByteString.Lazy as BSL
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Shelley.Ledger (shelleyLedgerState)
import RIO
import qualified RIO.Map as Map

encodeNewEpochState :: Crypto c => ExtLedgerState (CardanoBlock c) -> Encoding
encodeNewEpochState = applyNewEpochState toCBOR toCBOR

writeNewEpochState :: (Crypto c, MonadIO m) => FilePath -> ExtLedgerState (CardanoBlock c) -> m ()
writeNewEpochState fp = liftIO . BSL.writeFile fp . Plain.serialize . encodeNewEpochState

applyNewEpochState
  :: Crypto c
  => (ChainValidationState -> a)
  -> (forall era. ToCBOR (NewEpochState era)  => NewEpochState era -> a)
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
  => (forall era. (EraTxOut era, EraGovernance era) => NewEpochState era -> a)
  -> ExtLedgerState (CardanoBlock c)
  -> Maybe a
applyNonByronNewEpochState f extLedgerState =
  case ledgerState extLedgerState of
    LedgerStateByron _ -> Nothing
    LedgerStateShelley ls -> Just $ f (shelleyLedgerState ls)
    LedgerStateAllegra ls -> Just $ f (shelleyLedgerState ls)
    LedgerStateMary ls -> Just $ f (shelleyLedgerState ls)
    LedgerStateAlonzo ls -> Just $ f (shelleyLedgerState ls)
    LedgerStateBabbage ls -> Just $ f (shelleyLedgerState ls)
    LedgerStateConway ls -> Just $ f (shelleyLedgerState ls)

lookupStakeCredentials
  :: Set (Credential 'Staking (EraCrypto era))
  -> NewEpochState era
  -> UM.StakeCredentials (EraCrypto era)
lookupStakeCredentials creds nes =
  let um = dsUnified (certDState (lsCertState (esLState (nesEs nes))))
   in UM.domRestrictedStakeCredentials creds um

lookupReward :: Set (Credential 'Staking (EraCrypto era)) -> NewEpochState era -> Maybe Coin
lookupReward creds nes = guard (Map.null rewards) >> pure (fold rewards)
  where
    rewards = scRewards $ lookupStakeCredentials creds nes

-- lookupReward :: Set (Credential 'Staking (EraCrypto era)) -> NewEpochState era -> Maybe Coin
-- lookupReward = undefined
