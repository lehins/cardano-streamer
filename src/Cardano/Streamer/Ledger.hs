{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Streamer.Ledger where

import Cardano.Crypto.Hash (hashToTextAsHex)
import Cardano.Ledger.Address
import Cardano.Ledger.Allegra.Scripts
import Cardano.Ledger.Alonzo.Scripts
import Cardano.Ledger.Alonzo.Tx
import Cardano.Ledger.Api.Era
import Cardano.Ledger.Babbage.Collateral (collOuts)
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary (EncCBOR, ToCBOR)
import Cardano.Ledger.Coin
import Cardano.Ledger.Conway.Core
import qualified Cardano.Ledger.Conway.Rules as Conway
import Cardano.Ledger.Conway.TxCert
import Cardano.Ledger.Credential
import Cardano.Ledger.DRep
import Cardano.Ledger.MemoBytes
import Cardano.Ledger.Plutus.Language
import Cardano.Ledger.PoolParams
import Cardano.Ledger.Shelley.LedgerState
import Cardano.Ledger.Shelley.Rewards (aggregateRewards)
import qualified Cardano.Ledger.Shelley.Rules as Shelley
import Cardano.Ledger.Shelley.Scripts
import Cardano.Ledger.State
import Cardano.Ledger.UMap
import Cardano.Streamer.Common
import Control.State.Transition.Extended
import Data.Aeson
import Data.Aeson.Types (toJSONKeyText)
import qualified Data.ByteString.Short as SBS
import Data.Csv
import qualified Data.Foldable as F
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import qualified Data.Set as Set

data AppLanguage
  = AppNativeLanguage
  | AppPlutusLanguage !Language
  deriving (Eq, Ord, Show)

instance Enum AppLanguage where
  fromEnum = \case
    AppNativeLanguage -> 0
    AppPlutusLanguage l -> succ $ fromEnum l
  toEnum = \case
    0 -> AppNativeLanguage
    n -> AppPlutusLanguage $ toEnum (pred n)

instance Bounded AppLanguage where
  minBound = AppNativeLanguage
  maxBound = AppPlutusLanguage maxBound

appLanguageToText :: AppLanguage -> Text
appLanguageToText = \case
  AppNativeLanguage -> "Native"
  AppPlutusLanguage lang -> languageToText lang

instance ToJSON AppLanguage where
  toJSON = String . appLanguageToText

instance ToJSONKey AppLanguage where
  toJSONKey = toJSONKeyText appLanguageToText

data AppScript where
  AppMultiSig :: MultiSig era -> AppScript
  AppTimelock :: Timelock era -> AppScript
  AppPlutusScript :: PlutusLanguage l => Plutus l -> AppScript

appLanguage :: AppScript -> AppLanguage
appLanguage = \case
  AppMultiSig{} -> AppNativeLanguage
  AppTimelock{} -> AppNativeLanguage
  AppPlutusScript p -> AppPlutusLanguage (plutusLanguage p)

appScriptBytes :: AppScript -> ShortByteString
appScriptBytes = \case
  AppMultiSig ns -> getMemoRawBytes ns
  AppTimelock ns -> getMemoRawBytes ns
  AppPlutusScript ps -> unPlutusBinary (plutusBinary ps)

appScriptSize :: AppScript -> Int
appScriptSize = SBS.length . appScriptBytes

class
  ( EraSegWits era
  , EraGov era
  , EraUTxO era
  , EraStake era
  , EraCertState era
  , ToCBOR (NewEpochState era)
  , EncCBOR (NewEpochState era)
  ) =>
  EraApp era
  where
  appScript :: Script era -> AppScript

  appOutScriptsTxBody :: TxBody era -> [Script era]
  appOutScriptsTxBody = mempty

  appRefScriptsTxBody :: UTxO era -> TxBody era -> [Script era]
  appRefScriptsTxBody = mempty

  -- | All of the unspent outputs produced by the transaction
  utxoTx :: Tx era -> UTxO era

  getRewardsFromEvents ::
    proxy era ->
    Event (EraRule "TICK" era) ->
    Map (Credential 'Staking) Coin
  default getRewardsFromEvents ::
    ( Event (EraRule "TICK" era) ~ Shelley.ShelleyTickEvent era
    , Event (EraRule "NEWEPOCH" era) ~ Shelley.ShelleyNewEpochEvent era
    ) =>
    proxy era ->
    Event (EraRule "TICK" era) ->
    Map (Credential 'Staking) Coin
  getRewardsFromEvents _ (Shelley.TickNewEpochEvent (Shelley.TotalRewardEvent _ rs)) =
    aggregateRewards (ProtVer (eraProtVerLow @era) 0) rs
  getRewardsFromEvents _ _ = Map.empty

  getCreatedOutputsTx :: Tx era -> UTxO era

  updateCertificates :: SlotNo -> LostAda -> TxCert era -> LostAda

shelleyUpdateCertitifcates :: ShelleyEraTxCert era => SlotNo -> LostAda -> TxCert era -> LostAda
shelleyUpdateCertitifcates slotNo curLostAda = \case
  RegTxCert cred ->
    let updateSlotNo _ oldLa _newLa =
          oldLa{laRegisteredAtSlotNo = SJust slotNo}
        newLostAda =
          LostAdaAccount
            { laAccount = cred
            , laRegisteredAtSlotNo = SJust slotNo
            , laUnregisteredAtSlotNo = SNothing
            , laStakePoolDelegation = SNothing
            , laDRepDelegation = SNothing
            , laLastDelegationSlotNo = SNothing
            , laLastWithdrawalSlotNo = SNothing
            , laLastAddressOwnerActivitySlotNo = SNothing
            , laLastAddressActivitySlotNo = SNothing
            , laCurrentStake = mempty
            , laCurrentBalance = mempty
            }
     in curLostAda
          { laAccounts =
              snd $
                Map.insertLookupWithKey updateSlotNo cred newLostAda (laAccounts curLostAda)
          }
  UnRegTxCert cred ->
    let updateSlotNo la =
          la
            { laUnregisteredAtSlotNo = SJust slotNo
            , laStakePoolDelegation = SNothing
            }
     in curLostAda
          { laAccounts = Map.adjust updateSlotNo cred (laAccounts curLostAda)
          }
  DelegStakeTxCert cred poolId ->
    let updateSlotNo la =
          la
            { laLastDelegationSlotNo = SJust slotNo
            , laStakePoolDelegation = SJust poolId
            }
     in curLostAda
          { laAccounts = Map.adjust updateSlotNo cred (laAccounts curLostAda)
          }
  RegPoolTxCert pp ->
    let updateSlotNo la =
          la
            { laLastAddressOwnerActivitySlotNo = SJust slotNo
            }
     in curLostAda
          { laAccounts = Map.adjust updateSlotNo (raCredential (ppRewardAccount pp)) (laAccounts curLostAda)
          }
  _ -> curLostAda

instance EraApp ShelleyEra where
  appScript = AppMultiSig
  utxoTx tx = txouts (tx ^. bodyTxL)
  getCreatedOutputsTx tx = txouts (tx ^. bodyTxL)
  updateCertificates = shelleyUpdateCertitifcates

instance EraApp AllegraEra where
  appScript = AppTimelock
  utxoTx tx = txouts (tx ^. bodyTxL)
  getCreatedOutputsTx tx = txouts (tx ^. bodyTxL)
  updateCertificates = shelleyUpdateCertitifcates

instance EraApp MaryEra where
  appScript = AppTimelock
  utxoTx tx = txouts (tx ^. bodyTxL)
  getCreatedOutputsTx tx = txouts (tx ^. bodyTxL)
  updateCertificates = shelleyUpdateCertitifcates

instance EraApp AlonzoEra where
  appScript s = fromJust (appTimelockScript s <|> appPlutusScript s)
  utxoTx tx = txouts (tx ^. bodyTxL)
  getCreatedOutputsTx tx =
    case tx ^. isValidTxL of
      IsValid True -> txouts (tx ^. bodyTxL)
      IsValid False -> mempty
  updateCertificates = shelleyUpdateCertitifcates

instance EraApp BabbageEra where
  appScript s = fromJust (appTimelockScript s <|> appPlutusScript s)
  appOutScriptsTxBody = babbageScriptOutsTxBody
  appRefScriptsTxBody = getAllReferenceScripts
  utxoTx tx
    | tx ^. isValidTxL == IsValid True = txouts (tx ^. bodyTxL)
    | otherwise = collOuts (tx ^. bodyTxL)
  getCreatedOutputsTx tx =
    case tx ^. isValidTxL of
      IsValid True -> txouts (tx ^. bodyTxL)
      IsValid False -> collOuts (tx ^. bodyTxL)
  updateCertificates = shelleyUpdateCertitifcates

instance EraApp ConwayEra where
  appScript s = fromJust (appTimelockScript s <|> appPlutusScript s)
  appOutScriptsTxBody = babbageScriptOutsTxBody
  appRefScriptsTxBody = getAllReferenceScripts
  utxoTx tx
    | tx ^. isValidTxL == IsValid True = txouts (tx ^. bodyTxL)
    | otherwise = collOuts (tx ^. bodyTxL)
  getRewardsFromEvents _ (Shelley.TickNewEpochEvent (Conway.TotalRewardEvent _ rs)) =
    aggregateRewards (ProtVer (eraProtVerLow @ConwayEra) 0) rs
  getRewardsFromEvents _ _ = Map.empty
  getCreatedOutputsTx tx =
    case tx ^. isValidTxL of
      IsValid True -> txouts (tx ^. bodyTxL)
      IsValid False -> collOuts (tx ^. bodyTxL)
  updateCertificates = conwayUpdateCertificates

conwayUpdateCertificates ::
  forall era.
  ConwayEraTxCert era =>
  SlotNo ->
  LostAda ->
  TxCert era ->
  LostAda
conwayUpdateCertificates slotNo curLostAda = \case
  RegDepositTxCert cred _ ->
    shelleyUpdateCertitifcates @era slotNo curLostAda (RegTxCert cred)
  UnRegDepositTxCert cred _ ->
    shelleyUpdateCertitifcates @era slotNo curLostAda (UnRegTxCert cred)
  DelegTxCert cred delegatee ->
    case delegatee of
      DelegStake poolId ->
        shelleyUpdateCertitifcates @era slotNo curLostAda (DelegStakeTxCert cred poolId)
      DelegVote dRep ->
        let updateSlotNo la =
              la
                { laLastDelegationSlotNo = SJust slotNo
                , laDRepDelegation = SJust dRep
                }
         in curLostAda
              { laAccounts = Map.adjust updateSlotNo cred (laAccounts curLostAda)
              }
      DelegStakeVote poolId dRep ->
        let lostAda = shelleyUpdateCertitifcates @era slotNo curLostAda (DelegStakeTxCert cred poolId)
         in conwayUpdateCertificates @era slotNo lostAda (DelegTxCert cred (DelegVote dRep))
  RegDepositDelegTxCert cred delegatee _ ->
    let lostAda = shelleyUpdateCertitifcates @era slotNo curLostAda (RegTxCert cred)
     in conwayUpdateCertificates @era slotNo lostAda (DelegTxCert cred delegatee)
  cert -> shelleyUpdateCertitifcates @era slotNo curLostAda cert

appTimelockScript ::
  (EraScript era, NativeScript era ~ Timelock era) => Script era -> Maybe AppScript
appTimelockScript script = AppTimelock <$> getNativeScript script

appPlutusScript :: AlonzoEraScript era => Script era -> Maybe AppScript
appPlutusScript script = do
  ps <- toPlutusScript script
  Just $ withPlutusScript ps AppPlutusScript

-- | Get all scripts referenced by the transaction, regardless if they are used or not.
getAllReferenceScripts ::
  BabbageEraTxBody era =>
  UTxO era ->
  TxBody era ->
  [Script era]
getAllReferenceScripts (UTxO mp) txBody =
  mapMaybe refScript $ Map.elems $ Map.restrictKeys mp inputs
  where
    inputs = (txBody ^. referenceInputsTxBodyL) `Set.union` (txBody ^. inputsTxBodyL)
    refScript txOut =
      case txOut ^. referenceScriptTxOutL of
        SNothing -> Nothing
        SJust script -> Just script

-- plutusScriptTxWits :: EraApp era c => TxWits era -> Map (ScriptHash c) PlutusWithLanguage
-- plutusScriptTxWits txWits =
--   Map.mapMaybe appPlutusScriptWithLanguage (txWits ^. scriptTxWitsL)

appScriptTxWits :: EraApp era => TxWits era -> Map ScriptHash AppScript
appScriptTxWits txWits = Map.map appScript (txWits ^. scriptTxWitsL)

-- | Plutus Scripts from outputs
babbageScriptOutsTxBody ::
  BabbageEraTxBody era =>
  TxBody era ->
  [Script era]
babbageScriptOutsTxBody txBody =
  mapMaybe getOutputScript $ toList (txBody ^. outputsTxBodyL)
  where
    getOutputScript txOut =
      case txOut ^. referenceScriptTxOutL of
        SNothing -> Nothing
        SJust script -> pure script

-- -- | Plutus Scripts from outputs
-- plutusScriptOutsTxBody
--   :: (BabbageEraTxBody era, EraApp era c)
--   => TxBody era
--   -> Map (ScriptHash c) PlutusWithLanguage
-- plutusScriptOutsTxBody txBody =
--   Map.fromList $ mapMaybe getOutputScript $ toList (txBody ^. outputsTxBodyL)
--   where
--     getOutputScript txOut =
--       case txOut ^. referenceScriptTxOutL of
--         SNothing -> Nothing
--         SJust script ->
--           pwl <- appPlutusScript script
--           pure (hashScript script, pwl)

-- plutusScriptTx
--   :: (EraApp era, BabbageEraTxBody era)
--   => Tx era
--   -> Map ScriptHash PlutusWithLanguage
-- plutusScriptTx tx = plutusScriptTxWits (tx ^. witsTxL) <> plutusScriptOutsTxBody (tx ^. bodyTxL)

-- plutusScriptsPerLanguage :: Foldable f => f PlutusWithLanguage -> Map Language (Set PlutusBinary)
-- plutusScriptsPerLanguage = foldl' combinePlutusScripts mempty
--   where
--     combinePlutusScripts acc = \case
--       PlutusWithLanguage p ->
--         Map.insertWith (<>) (plutusLanguage p) (Set.singleton (plutusBinary p)) acc

scriptsPerLanguage :: Foldable f => f AppScript -> Map AppLanguage [AppScript]
scriptsPerLanguage = foldl' combinePlutusScripts mempty
  where
    combinePlutusScripts acc script =
      Map.insertWith (<>) (appLanguage script) [script] acc

-- | Produce all of the reference scripts that are evaluated and all of the provided
-- reference scripts. It is possible for the list to contain duplicate scripts and scripts
-- that are not even in the Map.
refScriptsTxBody ::
  EraApp era =>
  UTxO era ->
  TxBody era ->
  (Map ScriptHash AppScript, [AppScript])
refScriptsTxBody utxo txBody =
  (refScriptsUsed, map appScript refScripts)
  where
    refScripts = appRefScriptsTxBody utxo txBody
    scriptHashesNeeded = getScriptsHashesNeeded $ getScriptsNeeded utxo txBody
    refScriptsProvided = Map.fromList [(hashScript s, s) | s <- refScripts]
    refScriptsUsed =
      Map.map appScript $
        (refScriptsProvided `Map.restrictKeys` scriptHashesNeeded)
          `Map.union` Map.filter isNativeScript refScriptsProvided

outScriptTxBody :: EraApp era => TxBody era -> [AppScript]
outScriptTxBody = map appScript . appOutScriptsTxBody

-- plutusOutScriptTxBody :: EraApp era c => TxBody era -> [PlutusWithLanguage]
-- plutusOutScriptTxBody = mapMaybe appPlutusScriptWithLanguage . appOutScriptsTxBody

-- data PlutusWithLanguage where
--   PlutusWithLanguage :: PlutusLanguage l => Plutus l -> PlutusWithLanguage

-- appPlutusScriptWithLanguage :: EraApp era c => Script era -> Maybe PlutusWithLanguage
-- appPlutusScriptWithLanguage script =
--   case appScript script of
--     AppPlutusScript plutus -> Just $ PlutusWithLanguage plutus
--     _ -> Nothing

newtype LostAda = LostAda
  { laAccounts :: Map (Credential 'Staking) LostAdaAccount
  -- TODO: Add total ADA in UTxO
  }
  deriving (Show, Eq)

instance Semigroup LostAda where
  la1 <> la2 =
    LostAda
      { laAccounts = laAccounts la1 <> laAccounts la2
      }

instance Monoid LostAda where
  mempty = LostAda mempty

data LostAdaAccount = LostAdaAccount
  { laAccount :: !(Credential 'Staking)
  -- ^ Unique identifier of an account
  , laRegisteredAtSlotNo :: !(StrictMaybe SlotNo)
  -- ^ Slot number when an account was registered last (it is possible to register and unregister an
  -- account any number of times). It is a Maybe type, because we also track activity for unregistered
  -- accounts, just in case they do get registered later.
  , laUnregisteredAtSlotNo :: !(StrictMaybe SlotNo)
  -- ^ Slot number when account was unregistered last.  If this number is higher than
  -- `laRegisteredAtSlotNo` then this account is no longer unregistered and `laCurrentBalance` will
  -- be missing.
  , laStakePoolDelegation :: !(StrictMaybe (KeyHash 'StakePool))
  -- ^ Delagation to a Stake Pool
  , laDRepDelegation :: !(StrictMaybe DRep)
  -- ^ Delagation to a DRep
  , laLastDelegationSlotNo :: !(StrictMaybe SlotNo)
  -- ^ Slot number when the last time account submitted a delegation certificate either to a DRep or
  -- to a StakePool.
  --
  -- This serves as a hard proof of when account was accessed last for the purpose of delegation
  , laLastWithdrawalSlotNo :: !(StrictMaybe SlotNo)
  -- ^ Slot number when a witness was required for the purpose of withdrawal
  --
  -- This serves as a hard proof of when account was accessed last for the purpose of withdrawing
  -- rewards
  , laLastAddressOwnerActivitySlotNo :: !(StrictMaybe SlotNo)
  -- ^ Last slot number when a transfer was made to an address associated with a reward account:
  --
  -- * spending an output from an address with staking credential
  -- * using account as a reward account for by stake pool
  --
  -- This serves as an indirect proof of when account was used last. This activities are usually
  -- performed by an owner of the account, but there is no cryptographic proof that it was indeed
  -- done by the owner.
  , laLastAddressActivitySlotNo :: !(StrictMaybe SlotNo)
  -- ^ Last slot number when a transfer was made to an address associated with a reward account:
  --
  -- * creating an output with an address that contains this reward account
  --
  -- This serves as a potential indicator that an account owner is still active, since this action
  -- means that the owner of an account received funds from someone.
  , laCurrentStake :: !(StrictMaybe Coin)
  -- ^ Amount of stake asssociated with an account today. When missing it means there is not a
  -- single unspent output in the UTxO with this account.
  , laCurrentBalance :: !(StrictMaybe Coin)
  -- ^ Latest balance of the account. When missing it means the account is no longer registered.
  }
  deriving (Eq, Show, Generic)

instance ToRecord LostAdaAccount
instance ToNamedRecord LostAdaAccount
instance DefaultOrdered LostAdaAccount

instance ToField r => ToField (StrictMaybe r) where
  toField = toField . strictMaybeToMaybe

instance ToField (Credential r) where
  toField = toField . credToText

instance ToField (KeyHash r) where
  toField (KeyHash hash) = toField $ hashToTextAsHex hash

instance ToField DRep where
  toField = \case
    DRepAlwaysAbstain -> "drep-alwaysAbstain"
    DRepAlwaysNoConfidence -> "drep-alwaysNoConfidence"
    DRepCredential cred -> toField $ "drep-" <> credToText cred

accLostAdaTx ::
  EraApp era =>
  SlotNo ->
  (LostAda, UTxO era) ->
  Tx era ->
  (LostAda, UTxO era)
accLostAdaTx slotNo (!lostAda, !(UTxO utxo)) tx =
  (newLostAda, newUTxO)
  where
    mSlotNo = SJust slotNo
    -- we don't need to remove spent outputs, since they can't be spent again anyways and this is a
    -- temporary data holder needed for a block, just so we can lookup those outputs in subsequent
    -- transaction in a block
    !newUTxO = UTxO (utxo `Map.union` createdOutputs)
    !newLostAda =
      flip (F.foldl' updateLastAddressOwner) spendableInputs $
        flip (F.foldl' updateLastAddressActivity) createdOutputs $
          flip (Map.foldlWithKey' updateLastWithdrawal) withdrawals $
            F.foldl' (updateCertificates slotNo) lostAda certs
    certs = tx ^. bodyTxL . certsTxBodyL
    Withdrawals withdrawals = tx ^. bodyTxL . withdrawalsTxBodyL
    spendableInputs = tx ^. bodyTxL . spendableInputsTxBodyF
    UTxO createdOutputs = getCreatedOutputsTx tx
    updateLastAddressActivity curLostAda txOut =
      case txOut ^. addrTxOutL of
        Addr _ _ (StakeRefBase cred) ->
          let updateSlotNo la =
                la{laLastAddressActivitySlotNo = mSlotNo}
           in curLostAda
                { laAccounts = Map.adjust updateSlotNo cred (laAccounts curLostAda)
                }
        _ -> curLostAda
    updateLastWithdrawal curLostAda RewardAccount{raCredential} _ =
      let updateSlotNo la =
            la{laLastWithdrawalSlotNo = mSlotNo}
       in curLostAda
            { laAccounts = Map.adjust updateSlotNo raCredential (laAccounts curLostAda)
            }
    updateLastAddressOwner curLostAda txIn =
      case Map.lookup txIn utxo of
        Nothing ->
          error $
            "Could not find " <> show txIn <> " from transaction " <> show tx <> " in the UTxO"
        Just txOut ->
          case txOut ^. addrTxOutL of
            Addr _ _ (StakeRefBase cred) ->
              let updateSlotNo la =
                    la{laLastAddressOwnerActivitySlotNo = mSlotNo}
               in curLostAda
                    { laAccounts = Map.adjust updateSlotNo cred (laAccounts curLostAda)
                    }
            _ -> curLostAda

accLostAdaTxs :: EraApp era => SlotNo -> LostAda -> NewEpochState era -> [Tx era] -> LostAda
accLostAdaTxs slotNo lostAda nes =
  fst . F.foldl' (accLostAdaTx slotNo) (lostAda, (nes ^. utxoL))

updateCurrentStakeAndRewards :: EraApp era => LostAda -> NewEpochState era -> LostAda
updateCurrentStakeAndRewards lostAda nes =
  lostAda
    { laAccounts = Map.foldlWithKey' updateStake lostAdaWithRewards instantStake
    }
  where
    lostAdaWithRewards = Map.foldlWithKey' updateRewards (laAccounts lostAda) $ umElems umap
    updateRewards la cred (UMElem (SJust rd) _ _ _) =
      Map.adjust (\laa -> laa{laCurrentBalance = SJust (fromCompact (rdReward rd))}) cred la
    updateRewards la _ _ = la
    umap = dsUnified (lsCertState (esLState (nesEs nes)) ^. certDStateL)
    updateStake la cred stake =
      Map.adjust (\laa -> laa{laCurrentStake = SJust (fromCompact stake)}) cred la
    instantStake = nes ^. instantStakeG . instantStakeCredentialsL
