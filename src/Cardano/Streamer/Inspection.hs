{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

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
  slotWithBlockInspection,
  unitPrefix,
  epochBlockStatsInspection,

  -- * AccountActivity
  BinTx (..),
  AccountActivity (..),
  emptyAccountActivity,
  addAccountActivity,
) where

import Cardano.Ledger.Api.Era
import Cardano.Ledger.Api.Tx
import Cardano.Ledger.BaseTypes (EpochNo, Globals, SlotNo (..), TxIx (..))
import Cardano.Ledger.Binary (serialize')
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.State
import Cardano.Ledger.TxIn
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.LedgerState
import qualified Data.Map.Strict as Map
import Data.Profunctor
import qualified Data.Set as Set
import Ouroboros.Consensus.Cardano.Block hiding (TxId)
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

slotWithBlockInspection :: SlotInspection b () () () () () SlotWithBlock
slotWithBlockInspection =
  noInspection{siFinal = \swb _ _ _ _ _ -> pure swb}

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

data BinTx = BinTx
  { binTxSlotNo :: !SlotNo
  , binTxIx :: !TxIx
  , binTxEpochNo :: !EpochNo
  , binTxCardanoEra :: !CardanoEra
  , binTxId :: !TxId
  , binTx :: !ByteString
  }
  deriving (Show)

data AccountActivity = AccountActivity
  { aaTxsWithAccount :: ![BinTx]
  -- ^ Transaction that have this account used in them
  , aaEpochsWithRewards :: !(Map EpochNo Coin)
  -- ^ Rewards distributed to an account
  , aaStakePools :: !(Set (KeyHash StakePool))
  -- ^ StakePools, to which account ever delegated to
  , aaTxsWithStakePools :: ![BinTx]
  -- ^ Transactions with StakePool activity, for any StakePool that this account ever deelegated to
  , aaDReps :: !(Set (Credential DRepRole))
  -- ^ DReps, to which account ever delegated to
  , aaTxsWithDReps :: ![BinTx]
  -- ^ Transactions with DRep activity, for any DRep that this account ever deelegated to
  }
  deriving (Show)

emptyAccountActivity :: AccountActivity
emptyAccountActivity =
  AccountActivity
    { aaTxsWithAccount = mempty
    , aaEpochsWithRewards = mempty
    , aaStakePools = mempty
    , aaTxsWithStakePools = mempty
    , aaDReps = mempty
    , aaTxsWithDReps = mempty
    }

addAccountActivity :: Credential Staking -> AccountActivity -> SlotWithBlock -> AccountActivity
addAccountActivity cred !accountActivity swb =
  withBlockTxs
    (const accountActivity)
    (foldl' accTxsAccountActivity accountActivity . zip [TxIx 0 ..])
    (biBlockComponent (swbBlockWithInfo swb))
  where
    accTxsAccountActivity ::
      AnyEraTx era =>
      AccountActivity ->
      (TxIx, Tx TopTx era) ->
      AccountActivity
    accTxsAccountActivity !acc (txIx, tx) =
      let acc' = foldl' (addFromCerts (toBinTx txIx tx)) acc (tx ^. bodyTxL . certsTxBodyL)
       in foldl' (addCredTx (toBinTx txIx tx)) acc' $
            map (unAccountId . aaAccountId) $
              Map.keys (unWithdrawals (tx ^. bodyTxL . withdrawalsTxBodyL))
    toBinTx ::
      forall era.
      AnyEraTx era =>
      TxIx ->
      Tx TopTx era ->
      BinTx
    toBinTx txIx tx =
      BinTx
        { binTxSlotNo = swbSlotNo swb
        , binTxIx = txIx
        , binTxEpochNo = swbEpochNo swb
        , binTxCardanoEra = swbCardanoEra swb
        , binTxId = txIdTx tx
        , binTx = serialize' (eraProtVerLow @era) tx
        }
    addCredTx tx !aa accId
      | accId == cred = aa{aaTxsWithAccount = tx : aaTxsWithAccount aa}
      | otherwise = aa
    addPoolTx tx poolId !aa
      | Set.member poolId (aaStakePools aa) =
          aa{aaTxsWithStakePools = tx : aaTxsWithStakePools aa}
      | otherwise = aa
    addDRepTx tx drep !aa
      | Set.member drep (aaDReps aa) =
          aa{aaTxsWithDReps = tx : aaTxsWithDReps aa}
      | otherwise = aa
    addFromCerts ::
      AnyEraTx era =>
      BinTx ->
      AccountActivity ->
      TxCert era ->
      AccountActivity
    addFromCerts tx !acc = \case
      AnyEraRegPoolTxCert pp ->
        -- If a pool that was previously delegated to re-registers, we want to know about it
        addPoolTx tx (sppId pp) acc
      AnyEraRetirePoolTxCert poolId _ ->
        -- If a pool that was previously delegated to retires, we want to know about it
        addPoolTx tx poolId acc
      AnyEraRegTxCert accId -> addCredTx tx acc accId
      AnyEraUnRegTxCert accId -> addCredTx tx acc accId
      AnyEraRegDepositTxCert accId _ -> addCredTx tx acc accId
      AnyEraUnRegDepositTxCert accId _ -> addCredTx tx acc accId
      AnyEraDelegTxCert accId delegatee -> addDelegatee accId delegatee (addCredTx tx acc accId)
      AnyEraRegDepositDelegTxCert accId delegatee _ ->
        addDelegatee accId delegatee (addCredTx tx acc accId)
      AnyEraAuthCommitteeHotKeyTxCert{} -> acc
      AnyEraResignCommitteeColdTxCert{} -> acc
      AnyEraRegDRepTxCert drep _ _ -> addDRepTx tx drep acc
      AnyEraUnRegDRepTxCert drep _ -> addDRepTx tx drep acc
      AnyEraUpdateDRepTxCert drep _ -> addDRepTx tx drep acc
      where
        addDelegatee accId
          | accId == cred = \case
              DelegStake poolId -> addStakePool poolId
              DelegVote (DRepCredential drep) -> addDRep drep
              DelegStakeVote poolId (DRepCredential drep) -> addStakePool poolId . addDRep drep
              _ -> id
          | otherwise = \_ -> id
        addStakePool poolId aa = aa{aaStakePools = Set.insert poolId (aaStakePools aa)}
        addDRep drep aa = aa{aaDReps = Set.insert drep (aaDReps aa)}
    _creds = Set.singleton $ cred
