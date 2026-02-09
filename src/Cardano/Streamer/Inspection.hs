{-# LANGUAGE GADTs #-}
{-# LANGUAGE NamedFieldPuns #-}

module Cardano.Streamer.Inspection (
  -- * Inspector
  SlotInspector (..),

  -- * Slot with Block
  SlotWithBlock (..),
  swbCardanoEra,
  swbSlotNo,
  swbEpochNo,
  swbGlobals,
  isFirstSlotOfNewEra,
  isFirstSlotOfNewEpoch,

  -- * Slot Inspection
  SlotInspection (..),
  noInspection,
  unitPrefix,
  epochBlockStatsInspection,
) where

import Cardano.Ledger.BaseTypes (EpochNo, Globals, SlotNo (..))
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState
import Data.Profunctor
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (TopLevelConfig (..))
import Ouroboros.Consensus.Ledger.Basics (ComputeLedgerEvents (..), LedgerResult (..))
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerState, Ticked)
import Ouroboros.Consensus.Ledger.Tables.MapKind (DiffMK, ValuesMK)

data SlotInspector b r where
  SlotInspector :: SlotInspection b dec ldbPull tick appBlock ldbPush r -> SlotInspector b r

instance Functor (SlotInspector b) where
  fmap f (SlotInspector si) = SlotInspector (fmap f si)

instance Profunctor SlotInspector where
  lmap f (SlotInspector si) =
    SlotInspector $
      si
        { siDecodeBlock = \bwi action -> siDecodeBlock si (fmap (fmap f) bwi) action
        }
  rmap = fmap

-- | This data type contains all of the relevant information from a slot transition that had a Block applied to.
data SlotWithBlock = SlotWithBlock
  { swbRawBlock :: !LByteString
  -- ^ Raw bytes of a serialized block
  , swbBlockWithInfo :: !(BlockWithInfo (CardanoBlock StandardCrypto))
  -- ^ Decoded block with extra information
  , swbPrevExtLedgerState :: !(ExtLedgerState (CardanoBlock StandardCrypto) ValuesMK)
  -- ^ Ledger state from some prior slot when the previous block was applied
  , swbTickExtLedgerState :: !(Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) DiffMK)
  -- ^ Ledger state from the current slot after TICK rule was executed
  , swbNewExtLedgerState :: !(ExtLedgerState (CardanoBlock StandardCrypto) DiffMK)
  -- ^ Final ledger state at the end of this slot after block was applied to
  -- `sTickExtLedgerState`.
  }

swbCardanoEra :: SlotWithBlock -> CardanoEra
swbCardanoEra = blockCardanoEra . biBlockComponent . swbBlockWithInfo

swbSlotNo :: SlotWithBlock -> SlotNo
swbSlotNo = biSlotNo . swbBlockWithInfo

swbEpochNo :: SlotWithBlock -> EpochNo
swbEpochNo = snd . tickedExtLedgerStateEpochNo . swbTickExtLedgerState

swbGlobals :: SlotWithBlock -> TopLevelConfig (CardanoBlock StandardCrypto) -> Maybe Globals
swbGlobals swb = globalsFromLedgerConfig (swbCardanoEra swb) (swbNewExtLedgerState swb)

-- | Returns `True` if there was a transtion to a new epoch in this slot
isFirstSlotOfNewEpoch :: HasCallStack => SlotWithBlock -> Bool
isFirstSlotOfNewEpoch swb =
  isTrueNextEnum (extLedgerStateEpochNo (swbPrevExtLedgerState swb)) (swbEpochNo swb)

isFirstSlotOfNewEra :: HasCallStack => SlotWithBlock -> Bool
isFirstSlotOfNewEra swb =
  isTrueNextEnum (extLedgerStateCardanoEra (swbPrevExtLedgerState swb)) (swbCardanoEra swb)

isTrueNextEnum :: (Ord a, Enum a, Show a, HasCallStack) => a -> a -> Bool
isTrueNextEnum prev cur =
  case compare (succ prev) cur of
    EQ -> True
    GT | prev == cur -> False
    _ -> False
      -- error $
      --   "Unexpected previous: "
      --     <> show prev
      --     <> " with relation to the current: "
      --     <> show cur

data SlotInspection b dec ldbPull tick appBlock ldbPush r = SlotInspection
  { siDecodeBlock ::
      BlockWithInfo (LByteString, b) ->
      RIO App (CardanoBlock StandardCrypto) ->
      RIO App (dec, CardanoBlock StandardCrypto)
  , siLedgerDbLookup ::
      dec ->
      RIO App (ExtLedgerState (CardanoBlock StandardCrypto) ValuesMK) ->
      RIO App (ldbPull, ExtLedgerState (CardanoBlock StandardCrypto) ValuesMK)
  , siTick ::
      ldbPull ->
      ( ComputeLedgerEvents ->
        RIO
          App
          ( LedgerResult
              (ExtLedgerState (CardanoBlock StandardCrypto))
              (Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) DiffMK)
          )
      ) ->
      RIO App (tick, Ticked (ExtLedgerState (CardanoBlock StandardCrypto)) DiffMK)
  , siApplyBlock ::
      tick ->
      (ComputeLedgerEvents -> RIO App (ExtLedgerState (CardanoBlock StandardCrypto) DiffMK)) ->
      RIO App (appBlock, ExtLedgerState (CardanoBlock StandardCrypto) DiffMK)
  , siLedgerDbPush :: appBlock -> RIO App () -> RIO App ldbPush
  , siFinal ::
      SlotWithBlock ->
      dec ->
      ldbPull ->
      tick ->
      appBlock ->
      ldbPush ->
      RIO App r
  }

instance Functor (SlotInspection b dec ldbPull tick appBlock ldbPush) where
  fmap func si@SlotInspection{siFinal} =
    si{siFinal = \s a b c d e -> func <$> siFinal s a b c d e}

noInspection :: SlotInspection b () () () () () ()
noInspection =
  SlotInspection
    { siDecodeBlock = const unitPrefix
    , siLedgerDbLookup = const unitPrefix
    , siTick = \_ action -> (,) () . lrResult <$> action OmitLedgerEvents
    , siApplyBlock = \_ action -> unitPrefix $ action OmitLedgerEvents
    , siLedgerDbPush = \_ -> id
    , siFinal = \_ _ _ _ _ _ -> pure ()
    }

unitPrefix :: Functor f => f b -> f ((), b)
unitPrefix action = (,) () <$> action

epochBlockStatsInspection :: SlotInspection b () () () () () EpochBlockStats
epochBlockStatsInspection =
  noInspection
    { siFinal = \swb@SlotWithBlock{swbBlockWithInfo, swbTickExtLedgerState} _ _ _ _ _ ->
        let block = biBlockComponent swbBlockWithInfo
         in pure $!
              EpochBlockStats
                { ebsEpochNo = swbEpochNo swb
                , ebsBlockStats =
                    case blockLanguageRefScriptsStats swbTickExtLedgerState block of
                      (refScriptsStats, allRefScriptsStats) ->
                        BlockStats
                          { bsBlocksSize = fromIntegral (biBlockSize swbBlockWithInfo)
                          , bsScriptsStatsWits = languageStatsTxWits block
                          , esScriptsStatsOutScripts = languageStatsOutsTxBody block
                          , esScriptsStatsRefScripts = refScriptsStats
                          , esScriptsStatsAllRefScripts = allRefScriptsStats
                          }
                }
    }
