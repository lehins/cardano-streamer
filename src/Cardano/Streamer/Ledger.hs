{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Cardano.Streamer.Ledger where

import Cardano.Ledger.Allegra.Scripts
import Cardano.Ledger.Alonzo.Scripts
import Cardano.Ledger.Alonzo.Tx
import Cardano.Ledger.Api.Era
import Cardano.Ledger.Babbage.Collateral (collOuts)
import Cardano.Ledger.Babbage.Core
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary (EncCBOR, ToCBOR)
import Cardano.Ledger.Coin
import Cardano.Ledger.Conway.Governance
import Cardano.Ledger.Conway.Rules
import qualified Cardano.Ledger.Conway.Rules as Conway
import Cardano.Ledger.Credential
import Cardano.Ledger.MemoBytes
import Cardano.Ledger.Plutus.Language
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
import qualified Data.Map.Strict as Map
import Data.Maybe (fromJust)
import qualified Data.Set as Set
import Prettyprinter (Doc, defaultLayoutOptions, hsep, layoutPretty, viaShow)
import Prettyprinter.Render.Terminal (AnsiStyle, renderStrict)
import Test.Cardano.Ledger.Conway.TreeDiff (tableDoc)

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

  -- This function forces the DRep pulser
  reportRatification :: [GovActionId] -> NewEpochState era -> Maybe (NewEpochState era, Text)
  reportRatification _ = const Nothing

instance EraApp ShelleyEra where
  appScript = AppMultiSig
  utxoTx tx = txouts (tx ^. bodyTxL)

instance EraApp AllegraEra where
  appScript = AppTimelock
  utxoTx tx = txouts (tx ^. bodyTxL)

instance EraApp MaryEra where
  appScript = AppTimelock
  utxoTx tx = txouts (tx ^. bodyTxL)

instance EraApp AlonzoEra where
  appScript s = fromJust (appTimelockScript s <|> appPlutusScript s)
  utxoTx tx = txouts (tx ^. bodyTxL)

instance EraApp BabbageEra where
  appScript s = fromJust (appTimelockScript s <|> appPlutusScript s)
  appOutScriptsTxBody = babbageScriptOutsTxBody
  appRefScriptsTxBody = getAllReferenceScripts
  utxoTx tx
    | tx ^. isValidTxL == IsValid True = txouts (tx ^. bodyTxL)
    | otherwise = collOuts (tx ^. bodyTxL)

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
  reportRatification gaIds nes =
    let govActions = mapMaybe lookupGovAction gaIds
        lookupGovAction gaId =
          proposalsLookupId gaId $ nes ^. newEpochStateGovStateL . proposalsGovStateL
        pv = nes ^. nesEsL . curPParamsEpochStateL . ppProtocolVersionL
     in case nes ^. newEpochStateDRepPulsingStateL of
          DRPulsing (DRepPulser{..})
            | not (null govActions) ->
                let
                  !snap =
                    PulsingSnapshot
                      dpProposals
                      finalDRepDistr
                      dpDRepState
                      (Map.map individualTotalPoolStake $ unPoolDistr finalStakePoolDistr)
                  !leftOver = Map.drop dpIndex $ umElems dpUMap
                  (finalDRepDistr, finalStakePoolDistr) =
                    computeDRepDistr dpInstantStake dpDRepState dpProposalDeposits dpStakePoolDistr dpDRepDistr leftOver
                  !ratifyEnv =
                    RatifyEnv
                      { reInstantStake = dpInstantStake
                      , reStakePoolDistr = finalStakePoolDistr
                      , reDRepDistr = finalDRepDistr
                      , reDRepState = dpDRepState
                      , reCurrentEpoch = dpCurrentEpoch
                      , reCommitteeState = dpCommitteeState
                      , reDelegatees = dRepMap dpUMap
                      , rePoolParams = dpPoolParams
                      }
                  !ratifySig = RatifySignal dpProposals
                  !ratifyState =
                    RatifyState
                      { rsEnactState = dpEnactState
                      , rsEnacted = mempty
                      , rsExpired = mempty
                      , rsDelayed = False
                      }
                  !ratifyState' = runConwayRatify dpGlobals ratifyEnv ratifyState ratifySig
                  -- This will be the epoch number for the previous epoch, which is what we want.
                  -- It is because this, presumably, can only be triggered upon the very first TICK
                  -- in the epoch.
                  eNo = nes ^. nesELL
                  committee = nes ^. nesEsL . epochStateGovStateL . committeeGovStateL
                  members = foldMap committeeMembers committee
                  dRepRatio gas =
                    dRepAcceptedRatio
                      ratifyEnv
                      (gas ^. gasDRepVotesL)
                      (gasAction gas)
                  committeeRatio gas =
                    committeeAcceptedRatio
                      members
                      (gasCommitteeVotes gas)
                      (reCommitteeState ratifyEnv)
                      eNo
                  spoRatio gas = spoAcceptedRatio ratifyEnv gas pv
                  isDRepAccepted = dRepAccepted ratifyEnv ratifyState
                  isCommitteeAccepted = committeeAccepted ratifyEnv ratifyState
                  isSPOAccepted = spoAccepted ratifyEnv ratifyState
                  reportGovAction gas =
                    tableDoc
                      (Just "ACCEPTED RATIOS")
                      [
                        ( "DRep accepted ratio: " <> show (isDRepAccepted gas)
                        , viaShow (dRepRatio gas)
                        )
                      ,
                        ( "Committee accepted ratio: " <> show (isCommitteeAccepted gas)
                        , viaShow (committeeRatio gas)
                        )
                      ,
                        ( "SPO accepted ratio: " <> show (isSPOAccepted gas)
                        , viaShow (spoRatio gas)
                        )
                      ,
                        ( "prevActionAsExpected:"
                        , viaShow
                            ( prevActionAsExpected
                                gas
                                (ensPrevGovActionIds (rsEnactState ratifyState))
                            )
                        )
                      ,
                        ( "validCommitteeTerm"
                        , viaShow
                            ( validCommitteeTerm
                                (gasAction gas)
                                (ensCurPParams (rsEnactState ratifyState))
                                (reCurrentEpoch ratifyEnv)
                            )
                        )
                      , ("NotDelayed", viaShow (not (rsDelayed ratifyState)))
                      ,
                        ( "withdrawalCanWithdraw"
                        , viaShow
                            ( withdrawalCanWithdraw
                                (gasAction gas)
                                (ensTreasury (rsEnactState ratifyState))
                            )
                        )
                      ]
                  report =
                    ansiDocToText $
                      hsep
                        [ "============== " <> viaShow eNo <> " =============="
                        , hsep (map reportGovAction govActions)
                        , viaShow ratifyState'
                        ]
                 in
                  Just (nes & newEpochStateDRepPulsingStateL .~ DRComplete snap ratifyState', report)
          _ -> Nothing

ansiDocToText :: Doc AnsiStyle -> Text
ansiDocToText = renderStrict . layoutPretty defaultLayoutOptions

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
