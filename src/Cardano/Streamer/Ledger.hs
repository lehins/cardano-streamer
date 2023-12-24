{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}

module Cardano.Streamer.Ledger where

import Cardano.Ledger.Alonzo.Core
import Cardano.Ledger.Alonzo.Scripts
import Cardano.Ledger.Api.Era
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

  appRefScripts :: TxOut era -> Maybe (Script era)
  appRefScripts _ = Nothing

instance Crypto c => EraApp (ShelleyEra c) c
instance Crypto c => EraApp (AllegraEra c) c
instance Crypto c => EraApp (MaryEra c) c

instance Crypto c => EraApp (AlonzoEra c) c where
  appPlutusScript = alonzoAppPlutusScript

  --appRefScripts =

instance Crypto c => EraApp (BabbageEra c) c where
  appPlutusScript = alonzoAppPlutusScript

instance Crypto c => EraApp (ConwayEra c) c where
  appPlutusScript = alonzoAppPlutusScript

alonzoAppPlutusScript :: AlonzoEraScript era => Script era -> Maybe PlutusWithLanguage
alonzoAppPlutusScript script = do
  ps <- toPlutusScript script
  Just $ withPlutusScript ps PlutusWithLanguage

plutusScriptTxWits :: EraApp era c => TxWits era -> Map (ScriptHash c) PlutusWithLanguage
plutusScriptTxWits txWits =
  Map.mapMaybe appPlutusScript (txWits ^. scriptTxWitsL)

plutusScriptsPerLanguage :: Foldable f => f PlutusWithLanguage -> Map Language (Set PlutusBinary)
plutusScriptsPerLanguage = foldl' combinePlutusScripts mempty
  where
    combinePlutusScripts acc = \case
      PlutusWithLanguage p ->
        Map.insertWith (<>) (plutusLanguage p) (Set.singleton (plutusBinary p)) acc
