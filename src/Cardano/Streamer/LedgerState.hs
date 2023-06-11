{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Cardano.Streamer.LedgerState (encodeNewEpochState) where

import Cardano.Ledger.Binary.Plain (Encoding, toCBOR)
import Cardano.Ledger.Crypto
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Shelley.Ledger (shelleyLedgerState)
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block

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
