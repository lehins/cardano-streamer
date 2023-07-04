{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}

module Cardano.Streamer.LedgerState (encodeNewEpochState, applyNonByronNewEpochState) where

import Cardano.Ledger.Binary.Plain (Encoding, toCBOR)
import Cardano.Ledger.CertState
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.Shelley.LedgerState
import Cardano.Ledger.UMap as UM
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Shelley.Ledger (shelleyLedgerState)

encodeNewEpochState :: Crypto c => ExtLedgerState (CardanoBlock c) -> Encoding
encodeNewEpochState extLedgerState =
  case ledgerState extLedgerState of
    LedgerStateByron ls -> toCBOR (byronLedgerState ls)
    LedgerStateShelley ls -> toCBOR (shelleyLedgerState ls)
    LedgerStateAllegra ls -> toCBOR (shelleyLedgerState ls)
    LedgerStateMary ls -> toCBOR (shelleyLedgerState ls)
    LedgerStateAlonzo ls -> toCBOR (shelleyLedgerState ls)
    LedgerStateBabbage ls -> toCBOR (shelleyLedgerState ls)
    LedgerStateConway ls -> toCBOR (shelleyLedgerState ls)

applyNonByronNewEpochState
  :: Crypto c
  => (forall era. Era era => NewEpochState era -> a)
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

lookupReward :: Credential 'Staking (EraCrypto era) -> NewEpochState era -> Maybe Coin
lookupReward cred nes =
  let um = dsUnified (certDState (lsCertState (esLState (nesEs nes))))
   in fromCompact . rdReward <$> UM.lookup cred (UM.RewardDeposits um)
