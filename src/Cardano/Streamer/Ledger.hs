{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}

module Cardano.Streamer.Ledger where

import Cardano.Ledger.Alonzo.Scripts
import Cardano.Ledger.Api.Era
import Cardano.Ledger.Babbage.Core
import Cardano.Ledger.Babbage.UTxO
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary.Plain (ToCBOR)
import Cardano.Ledger.Plutus.Language
import Cardano.Ledger.Shelley.LedgerState (NewEpochState)
import Cardano.Ledger.UTxO
import Cardano.Streamer.Common
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set

data PlutusWithLanguage where
  PlutusWithLanguage :: PlutusLanguage l => Plutus l -> PlutusWithLanguage

class
  ( EraSegWits era
  , EraGov era
  , EraUTxO era
  , ToCBOR (NewEpochState era)
  , EraCrypto era ~ c
  ) =>
  EraApp era c
    | era -> c
  where
  appPlutusScript :: Script era -> Maybe PlutusWithLanguage
  appPlutusScript _ = Nothing

  appOutputScriptsTxBody :: TxBody era -> [Script era]
  appOutputScriptsTxBody _ = mempty

  appRefScriptsTxBody :: UTxO era -> TxBody era -> Map (ScriptHash (EraCrypto era)) (Script era)
  appRefScriptsTxBody _ = mempty

instance Crypto c => EraApp (ShelleyEra c) c
instance Crypto c => EraApp (AllegraEra c) c
instance Crypto c => EraApp (MaryEra c) c

instance Crypto c => EraApp (AlonzoEra c) c where
  appPlutusScript = alonzoAppPlutusScript

instance Crypto c => EraApp (BabbageEra c) c where
  appPlutusScript = alonzoAppPlutusScript
  appOutputScriptsTxBody = babbageScriptOutsTxBody
  appRefScriptsTxBody = babbageRefScriptsTxBody

instance Crypto c => EraApp (ConwayEra c) c where
  appPlutusScript = alonzoAppPlutusScript
  appOutputScriptsTxBody = babbageScriptOutsTxBody
  appRefScriptsTxBody = babbageRefScriptsTxBody

alonzoAppPlutusScript :: AlonzoEraScript era => Script era -> Maybe PlutusWithLanguage
alonzoAppPlutusScript script = do
  ps <- toPlutusScript script
  Just $ withPlutusScript ps PlutusWithLanguage

babbageRefScriptsTxBody
  :: BabbageEraTxBody era
  => UTxO era
  -> TxBody era
  -> Map (ScriptHash (EraCrypto era)) (Script era)
babbageRefScriptsTxBody utxo txBody =
  getReferenceScripts utxo $
    (txBody ^. referenceInputsTxBodyL) `Set.union` (txBody ^. inputsTxBodyL)

plutusScriptTxWits :: EraApp era c => TxWits era -> Map (ScriptHash c) PlutusWithLanguage
plutusScriptTxWits txWits =
  Map.mapMaybe appPlutusScript (txWits ^. scriptTxWitsL)

-- | Plutus Scripts from outputs
babbageScriptOutsTxBody
  :: BabbageEraTxBody era
  => TxBody era
  -> [Script era]
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
--   :: (EraApp era (EraCrypto era), BabbageEraTxBody era)
--   => Tx era
--   -> Map (ScriptHash (EraCrypto era)) PlutusWithLanguage
-- plutusScriptTx tx = plutusScriptTxWits (tx ^. witsTxL) <> plutusScriptOutsTxBody (tx ^. bodyTxL)

plutusScriptsPerLanguage :: Foldable f => f PlutusWithLanguage -> Map Language (Set PlutusBinary)
plutusScriptsPerLanguage = foldl' combinePlutusScripts mempty
  where
    combinePlutusScripts acc = \case
      PlutusWithLanguage p ->
        Map.insertWith (<>) (plutusLanguage p) (Set.singleton (plutusBinary p)) acc

plutusRefScriptTxBody
  :: EraApp era c => UTxO era -> TxBody era -> Map (ScriptHash c) PlutusWithLanguage
plutusRefScriptTxBody utxo txBody = refsProvided `Map.restrictKeys` scriptHashesNeeded
  where
    scriptHashesNeeded = getScriptsHashesNeeded $ getScriptsNeeded utxo txBody
    refsProvided = Map.mapMaybe appPlutusScript (appRefScriptsTxBody utxo txBody)

plutusOutScriptTxBody :: EraApp era c => TxBody era -> [PlutusWithLanguage]
plutusOutScriptTxBody = mapMaybe appPlutusScript . appOutputScriptsTxBody
