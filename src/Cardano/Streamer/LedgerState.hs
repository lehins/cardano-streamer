{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Cardano.Streamer.LedgerState (
  readNewEpochState,
  encodeNewEpochState,
  applyNonByronNewEpochState,
  applyNewEpochState,
  modifyLedgerState,
  applyTickedNewEpochStateWithBlock,
  applyTickedNewEpochStateWithTxs,
  tickedExtLedgerStateEpochNo,
  blockLanguageRefScriptsStats,
  lookupRewards,
  lookupTotalRewards,
  writeNewEpochState,
  extLedgerStateCardanoEra,
  extLedgerStateEpochNo,
  extractLedgerEvents,
  globalsFromLedgerConfig,
  detectNewRewards,
  EpochBlockStats (..),
  BlockStats (..),
  EpochStats (..),
  epochStatsToNamedCsv,
  toEpochStats,
  Tip (..),
  tipFromExtLedgerState,
  tipFromHeaderState,
  tipFromLedgerState,
  pattern TickedLedgerStateByron,
  pattern TickedLedgerStateShelley,
  pattern TickedLedgerStateAllegra,
  pattern TickedLedgerStateMary,
  pattern TickedLedgerStateAlonzo,
  pattern TickedLedgerStateBabbage,
  pattern TickedLedgerStateConway,
  pattern TickedLedgerStateDijkstra,
) where

import Cardano.Chain.Block as Byron (ChainValidationState (..))
import qualified Cardano.Chain.Genesis as Byron (GenesisHash (..))
import qualified Cardano.Chain.Slotting as Byron (
  EpochNumber (getEpochNumber),
  SlotNumber (unSlotNumber),
 )
import qualified Cardano.Chain.UTxO as B
import qualified Cardano.Chain.Update.Validation.Interface as Byron (State (currentEpoch))
import Cardano.Crypto.Hash (hashFromBytes)
import qualified Cardano.Crypto.Hashing as Byron (hashToBytes)
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary
import qualified Cardano.Ledger.Binary.Plain as Plain
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Shelley.LedgerState hiding (LedgerState)
import Cardano.Ledger.State
import Cardano.Ledger.UMap as UM
import Cardano.Ledger.Val
import Cardano.Slotting.EpochInfo.API (hoistEpochInfo)
import Cardano.Slotting.Slot
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.Ledger
import Control.Monad.Trans.Except (runExcept, withExceptT)
import Data.Aeson (ToJSON (..), defaultOptions, fieldLabelModifier, genericToJSON)
import qualified Data.ByteString.Lazy as BSL
import Data.Csv (Field, ToNamedRecord (..), header, namedRecord, toField, (.=))
import qualified Data.Map.Merge.Strict as Map
import Data.Maybe (fromJust)
import Data.SOP.BasicFunctors
import Data.SOP.Functors (Flip (..))
import Data.SOP.Strict
import Data.SOP.Telescope
import Ouroboros.Consensus.Byron.ByronHFC ()
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Config (TopLevelConfig (..))
import Ouroboros.Consensus.HardFork.Combinator.AcrossEras (
  OneEraLedgerEvent (..),
  OneEraTipInfo (..),
  PerEraLedgerConfig (..),
 )
import Ouroboros.Consensus.HardFork.Combinator.Basics (
  LedgerState (HardForkLedgerState),
  hardForkLedgerConfigPerEra,
 )
import Ouroboros.Consensus.HardFork.Combinator.Ledger
import Ouroboros.Consensus.HardFork.Combinator.PartialConfig
import Ouroboros.Consensus.HardFork.Combinator.State (epochInfoLedger)
import Ouroboros.Consensus.HardFork.Combinator.State.Types as State
import Ouroboros.Consensus.HeaderValidation (
  AnnTip (..),
  HeaderState (..),
  TipInfoIsEBB (..),
 )
import Ouroboros.Consensus.Ledger.Basics
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Protocol.Praos
import Ouroboros.Consensus.Protocol.TPraos
import Ouroboros.Consensus.Shelley.HFEras ()
import Ouroboros.Consensus.Shelley.Ledger.Block
import Ouroboros.Consensus.Shelley.Ledger.Ledger
import Ouroboros.Consensus.Shelley.Ledger.SupportsProtocol ()
import Ouroboros.Consensus.Shelley.ShelleyHFC ()
import Ouroboros.Consensus.TypeFamilyWrappers (WrapLedgerEvent (..), WrapTipInfo (..))
import qualified RIO.Map as Map
import qualified RIO.Text as T

-- TODO: expose in consensus from Ouroboros.Consensus.Cardano.Block
pattern TeleByron ::
  f ByronBlock ->
  Telescope g f (CardanoEras c)
pattern TeleByron x =
  TZ x

pattern TeleShelley ::
  g ByronBlock ->
  f (ShelleyBlock (TPraos c) ShelleyEra) ->
  Telescope g f (CardanoEras c)
pattern TeleShelley byron x =
  TS byron (TZ x)

pattern TeleAllegra ::
  g ByronBlock ->
  g (ShelleyBlock (TPraos c) ShelleyEra) ->
  f (ShelleyBlock (TPraos c) AllegraEra) ->
  Telescope g f (CardanoEras c)
pattern TeleAllegra byron shelley x =
  TS byron (TS shelley (TZ x))

pattern TeleMary ::
  g ByronBlock ->
  g (ShelleyBlock (TPraos c) ShelleyEra) ->
  g (ShelleyBlock (TPraos c) AllegraEra) ->
  f (ShelleyBlock (TPraos c) MaryEra) ->
  Telescope g f (CardanoEras c)
pattern TeleMary byron shelley allegra x =
  TS byron (TS shelley (TS allegra (TZ x)))

pattern TeleAlonzo ::
  g ByronBlock ->
  g (ShelleyBlock (TPraos c) ShelleyEra) ->
  g (ShelleyBlock (TPraos c) AllegraEra) ->
  g (ShelleyBlock (TPraos c) MaryEra) ->
  f (ShelleyBlock (TPraos c) AlonzoEra) ->
  Telescope g f (CardanoEras c)
pattern TeleAlonzo byron shelley allegra mary x =
  TS byron (TS shelley (TS allegra (TS mary (TZ x))))

pattern TeleBabbage ::
  g ByronBlock ->
  g (ShelleyBlock (TPraos c) ShelleyEra) ->
  g (ShelleyBlock (TPraos c) AllegraEra) ->
  g (ShelleyBlock (TPraos c) MaryEra) ->
  g (ShelleyBlock (TPraos c) AlonzoEra) ->
  f (ShelleyBlock (Praos c) BabbageEra) ->
  Telescope g f (CardanoEras c)
pattern TeleBabbage byron shelley allegra mary alonzo x =
  TS byron (TS shelley (TS allegra (TS mary (TS alonzo (TZ x)))))

pattern TeleConway ::
  g ByronBlock ->
  g (ShelleyBlock (TPraos c) ShelleyEra) ->
  g (ShelleyBlock (TPraos c) AllegraEra) ->
  g (ShelleyBlock (TPraos c) MaryEra) ->
  g (ShelleyBlock (TPraos c) AlonzoEra) ->
  g (ShelleyBlock (Praos c) BabbageEra) ->
  f (ShelleyBlock (Praos c) ConwayEra) ->
  Telescope g f (CardanoEras c)
pattern TeleConway byron shelley allegra mary alonzo babbage x =
  TS byron (TS shelley (TS allegra (TS mary (TS alonzo (TS babbage (TZ x))))))

pattern TeleDijkstra ::
  g ByronBlock ->
  g (ShelleyBlock (TPraos c) ShelleyEra) ->
  g (ShelleyBlock (TPraos c) AllegraEra) ->
  g (ShelleyBlock (TPraos c) MaryEra) ->
  g (ShelleyBlock (TPraos c) AlonzoEra) ->
  g (ShelleyBlock (Praos c) BabbageEra) ->
  g (ShelleyBlock (Praos c) ConwayEra) ->
  f (ShelleyBlock (Praos c) DijkstraEra) ->
  Telescope g f (CardanoEras c)
pattern TeleDijkstra byron shelley allegra mary alonzo babbage conway x =
  TS byron (TS shelley (TS allegra (TS mary (TS alonzo (TS babbage (TS conway (TZ x)))))))

{-# COMPLETE TeleByron, TeleShelley, TeleAllegra, TeleMary, TeleAlonzo, TeleBabbage, TeleConway, TeleDijkstra #-}

modifyLedgerState ::
  forall c mk.
  (ChainValidationState -> ChainValidationState) ->
  (forall era. EraApp era => NewEpochState era -> NewEpochState era) ->
  ExtLedgerState (CardanoBlock c) mk ->
  ExtLedgerState (CardanoBlock c) mk
modifyLedgerState fByronBased fShelleyBased extLedgerState =
  let modifyShelleyBased ::
        EraApp era =>
        ( Current (Flip LedgerState mk) (ShelleyBlock (p c) era) ->
          Telescope (K Past) (Current (Flip LedgerState mk)) (CardanoEras c)
        ) ->
        Current (Flip LedgerState mk) (ShelleyBlock (p c) era) ->
        ExtLedgerState (CardanoBlock c) mk
      modifyShelleyBased tele current =
        let Flip cs = currentState current
         in extLedgerState
              { ledgerState =
                  HardForkLedgerState $
                    State.HardForkState $
                      tele $
                        current
                          { currentState =
                              Flip $
                                cs
                                  { shelleyLedgerState = fShelleyBased (shelleyLedgerState cs)
                                  }
                          }
              }
   in case ledgerState extLedgerState of
        HardForkLedgerState (HardForkState (TeleByron current)) ->
          let Flip cs = currentState current
           in extLedgerState
                { ledgerState =
                    HardForkLedgerState $
                      State.HardForkState $
                        TeleByron $
                          current
                            { currentState =
                                Flip $
                                  cs
                                    { byronLedgerState = fByronBased (byronLedgerState cs)
                                    }
                            }
                }
        HardForkLedgerState (HardForkState (TeleShelley byron current)) ->
          modifyShelleyBased (TeleShelley byron) current
        HardForkLedgerState (HardForkState (TeleAllegra byron shelley current)) ->
          modifyShelleyBased (TeleAllegra byron shelley) current
        HardForkLedgerState (HardForkState (TeleMary byron shelley allegra current)) ->
          modifyShelleyBased (TeleMary byron shelley allegra) current
        HardForkLedgerState (HardForkState (TeleAlonzo byron shelley allegra mary current)) ->
          modifyShelleyBased (TeleAlonzo byron shelley allegra mary) current
        HardForkLedgerState (HardForkState (TeleBabbage byron shelley allegra mary alonzo current)) ->
          modifyShelleyBased (TeleBabbage byron shelley allegra mary alonzo) current
        HardForkLedgerState (HardForkState (TeleConway byron shelley allegra mary alonzo babbage current)) ->
          modifyShelleyBased (TeleConway byron shelley allegra mary alonzo babbage) current
        HardForkLedgerState
          (HardForkState (TeleDijkstra byron shelley allegra mary alonzo babbage conway current)) ->
            modifyShelleyBased (TeleDijkstra byron shelley allegra mary alonzo babbage conway) current

encodeNewEpochState :: ExtLedgerState (CardanoBlock c) mk -> Plain.Encoding
encodeNewEpochState = applyNewEpochState toCBOR (const toCBOR)

writeNewEpochState :: MonadIO m => FilePath -> ExtLedgerState (CardanoBlock c) mk -> m ()
writeNewEpochState fp = liftIO . BSL.writeFile fp . Plain.serialize . encodeNewEpochState

-- writeNewEpochState fp ls = liftIO $ do
--   let encNes = applyNewEpochState undefined $ \nes ->
--         let enc = toPlainEncoding (natVersion @9) . encCBOR
--             roundtripEncode um =
--               let
--                 e = enc um
--                 bsl = Plain.serialize e
--                in
--                 case decodeFullDecoder (natVersion @9) "UMap" decNoShareCBOR bsl :: Either DecoderError UMap of
--                   Left err -> Left $ show err
--                   Right um' -> if um /= um' then Left "Does not roundtrip" else Right e
--             go UMap{..} =
--               let sz = Map.size umPtrs
--                   (es1, es2) = Map.splitAt (sz `div` 2) umElems
--                   (ps1, ps2) = Map.splitAt (sz `div` 2) umPtrs
--                   (um1, um2) = (UMap es1 ps1, UMap es2 ps2)
--                in if sz <= 1
--                     then case roundtripEncode um2 of
--                       Left err -> error (show err) `seq` enc um2
--                       Right _ -> error "Didn't find"
--                     else case roundtripEncode um1 of
--                       Left err -> go um1
--                       Right _ -> go um2
--          in go $
--               nes
--                 ^. nesEpochStateL
--                 . esLStateL
--                 . lsCertStateL
--                 . certDStateL
--                 . dsUnifiedL
--   BSL.writeFile fp . Plain.serialize . encNes $ ls

readNewEpochState ::
  ( EraGov era
  , EraStake era
  , EraCertState era
  , EraTxOut era
  , DecCBOR (StashedAVVMAddresses era)
  , MonadIO m
  ) =>
  FilePath -> m (NewEpochState era)
readNewEpochState fp =
  liftIO (BSL.readFile fp) <&> Plain.decodeFull >>= \case
    Left exc -> throwIO exc
    Right res -> pure res

pattern TickedLedgerStateByron ::
  TransitionInfo ->
  Ticked (LedgerState ByronBlock) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateByron ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleByron (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateShelley ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) ShelleyEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateShelley ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleShelley _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateAllegra ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) AllegraEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateAllegra ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleAllegra _ _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateMary ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) MaryEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateMary ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleMary _ _ _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateAlonzo ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) AlonzoEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateAlonzo ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleAlonzo _ _ _ _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateBabbage ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (Praos c) BabbageEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateBabbage ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleBabbage _ _ _ _ _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateConway ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (Praos c) ConwayEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateConway ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleConway _ _ _ _ _ _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

pattern TickedLedgerStateDijkstra ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (Praos c) DijkstraEra)) mk ->
  Ticked (LedgerState (CardanoBlock c)) mk
pattern TickedLedgerStateDijkstra ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TeleDijkstra _ _ _ _ _ _ _ (State.Current{currentState = FlipTickedLedgerState st}))
      )

{-# COMPLETE
  TickedLedgerStateByron
  , TickedLedgerStateShelley
  , TickedLedgerStateAllegra
  , TickedLedgerStateMary
  , TickedLedgerStateAlonzo
  , TickedLedgerStateBabbage
  , TickedLedgerStateConway
  , TickedLedgerStateDijkstra
  #-}

data Tip = Tip
  { tipSlotNo :: SlotNo
  , tipPrevBlockNo :: BlockNo
  , tipPrevBlockHeaderHash :: Hash HASH EraIndependentBlockHeader
  }
  deriving (Eq, Ord, Show)

instance Display Tip where
  display Tip{..} =
    "Tip<SlotNo: "
      <> display tipSlotNo
      <> ", PrevBlockNo: "
      <> display tipPrevBlockNo
      <> ", PrevBlockHeaderHash: "
      <> display tipPrevBlockHeaderHash
      <> ">"

tipFromExtLedgerState ::
  ExtLedgerState (CardanoBlock StandardCrypto) mk -> Maybe Tip
tipFromExtLedgerState = tipFromHeaderState . headerState

globalsFromLedgerConfig ::
  CardanoEra ->
  ExtLedgerState (CardanoBlock StandardCrypto) mk ->
  TopLevelConfig (CardanoBlock StandardCrypto) ->
  Maybe Globals
globalsFromLedgerConfig era (ExtLedgerState ledgerState _) lc =
  globalsWithDummyEpochInfo <&> \globals -> globals{epochInfo = actualEpochInfo}
  where
    hardForkLedgerConfig = topLevelConfigLedger lc
    HardForkLedgerState hardForkState = ledgerState
    actualEpochInfo =
      hoistEpochInfo
        (runExcept . withExceptT (T.pack . show))
        $ epochInfoLedger hardForkLedgerConfig hardForkState
    globalsWithDummyEpochInfo =
      case getPerEraLedgerConfig $ hardForkLedgerConfigPerEra hardForkLedgerConfig of
        WrapPartialLedgerConfig _partialLedgerConfigByron
          :* WrapPartialLedgerConfig partialLedgerConfigShelley
          :* WrapPartialLedgerConfig partialLedgerConfigAllegra
          :* WrapPartialLedgerConfig partialLedgerConfigMary
          :* WrapPartialLedgerConfig partialLedgerConfigAlonzo
          :* WrapPartialLedgerConfig partialLedgerConfigBabbage
          :* WrapPartialLedgerConfig partialLedgerConfigConway
          :* WrapPartialLedgerConfig partialLedgerConfigDijkstra
          :* Nil ->
            case era of
              Byron -> Nothing
              Shelley -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigShelley
              Mary -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigAllegra
              Allegra -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigMary
              Alonzo -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigAlonzo
              Babbage -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigBabbage
              Conway -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigConway
              Dijkstra -> Just $ shelleyLedgerGlobals $ shelleyLedgerConfig partialLedgerConfigDijkstra

tipFromHeaderState ::
  HeaderState (CardanoBlock StandardCrypto) -> Maybe Tip
tipFromHeaderState hs = do
  AnnTip{..} <- withOriginToMaybe $ headerStateTip hs
  pure $
    Tip
      { tipSlotNo = annTipSlotNo
      , tipPrevBlockNo = annTipBlockNo
      , tipPrevBlockHeaderHash =
          case fromTip $ getOneEraTipInfo annTipInfo of
            TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (WrapTipInfo headerHash)))))))) -> unShelleyHash headerHash
            TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (WrapTipInfo headerHash))))))) -> unShelleyHash headerHash
            TS _ (TS _ (TS _ (TS _ (TS _ (TZ (WrapTipInfo headerHash)))))) -> unShelleyHash headerHash
            TS _ (TS _ (TS _ (TS _ (TZ (WrapTipInfo headerHash))))) -> unShelleyHash headerHash
            TS _ (TS _ (TS _ (TZ (WrapTipInfo headerHash)))) -> unShelleyHash headerHash
            TS _ (TS _ (TZ (WrapTipInfo headerHash))) -> unShelleyHash headerHash
            TS _ (TZ (WrapTipInfo headerHash)) -> unShelleyHash headerHash
            TZ (WrapTipInfo (TipInfoIsEBB (ByronHash headerHash) _)) ->
              fromJust . hashFromBytes $ Byron.hashToBytes headerHash
      }

tipFromLedgerState ::
  LedgerState (CardanoBlock StandardCrypto) mk -> Maybe Tip
tipFromLedgerState ledgerState =
  case ledgerState of
    LedgerStateByron ls -> toByronTip ls
    LedgerStateShelley ls -> toShelleyTip ls
    LedgerStateAllegra ls -> toShelleyTip ls
    LedgerStateMary ls -> toShelleyTip ls
    LedgerStateAlonzo ls -> toShelleyTip ls
    LedgerStateBabbage ls -> toShelleyTip ls
    LedgerStateConway ls -> toShelleyTip ls
    LedgerStateDijkstra ls -> toShelleyTip ls
  where
    fromByronHash = fromJust . hashFromBytes . Byron.hashToBytes
    toByronTip ls = do
      blockNo <- withOriginToMaybe $ byronLedgerTipBlockNo ls
      let chainValidationState = byronLedgerState ls
      pure $
        Tip
          { tipSlotNo = SlotNo $ Byron.unSlotNumber $ cvsLastSlot chainValidationState
          , tipPrevBlockNo = blockNo
          , tipPrevBlockHeaderHash =
              case cvsPreviousHash chainValidationState of
                Left (Byron.GenesisHash genesisHash) -> fromByronHash genesisHash
                Right headerHash -> fromByronHash headerHash
          }
    toShelleyTip ls = do
      ShelleyTip{..} <- withOriginToMaybe $ shelleyLedgerTip ls
      pure $
        Tip
          { tipSlotNo = shelleyTipSlotNo
          , tipPrevBlockNo = shelleyTipBlockNo
          , tipPrevBlockHeaderHash = unShelleyHash shelleyTipHash
          }

extLedgerStateCardanoEra :: ExtLedgerState (CardanoBlock c) mk -> CardanoEra
extLedgerStateCardanoEra = applyNewEpochState (const Byron) (\era _ -> era)

applyNewEpochState ::
  (ChainValidationState -> a) ->
  (forall era. EraApp era => CardanoEra -> NewEpochState era -> a) ->
  ExtLedgerState (CardanoBlock c) mk ->
  a
applyNewEpochState fByronBased fShelleyBased extLedgerState =
  case ledgerState extLedgerState of
    LedgerStateByron ls -> fByronBased (byronLedgerState ls)
    LedgerStateShelley ls -> fShelleyBased Shelley (shelleyLedgerState ls)
    LedgerStateAllegra ls -> fShelleyBased Allegra (shelleyLedgerState ls)
    LedgerStateMary ls -> fShelleyBased Mary (shelleyLedgerState ls)
    LedgerStateAlonzo ls -> fShelleyBased Alonzo (shelleyLedgerState ls)
    LedgerStateBabbage ls -> fShelleyBased Babbage (shelleyLedgerState ls)
    LedgerStateConway ls -> fShelleyBased Conway (shelleyLedgerState ls)
    LedgerStateDijkstra ls -> fShelleyBased Dijkstra (shelleyLedgerState ls)

applyNonByronNewEpochState ::
  (forall era. EraApp era => NewEpochState era -> a) ->
  ExtLedgerState (CardanoBlock c) mk ->
  Maybe a
applyNonByronNewEpochState f = applyNewEpochState (const Nothing) (\_ -> Just . f)

applyTickedNewEpochState ::
  (TransitionInfo -> ChainValidationState -> a) ->
  (forall era. EraApp era => TransitionInfo -> NewEpochState era -> a) ->
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  a
applyTickedNewEpochState fByronBased fShelleyBased tickedExtLedgerState =
  case tickedLedgerState tickedExtLedgerState of
    TickedLedgerStateByron ti ls -> fByronBased ti (tickedByronLedgerState ls)
    TickedLedgerStateShelley ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateAllegra ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateMary ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateAlonzo ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateBabbage ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateConway ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateDijkstra ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)

applyTickedNonByronNewEpochState ::
  (forall era. EraApp era => TransitionInfo -> NewEpochState era -> a) ->
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  Maybe a
applyTickedNonByronNewEpochState f =
  applyTickedNewEpochState (\_ _ -> Nothing) (\ti -> Just . f ti)

queryAccounts ::
  EraCertState era =>
  Set (Credential 'Staking) ->
  NewEpochState era ->
  Map (Credential 'Staking) (AccountState era)
queryAccounts creds nes =
  (nes ^. nesEpochStateL . esLStateL . lsCertStateL . certDStateL . accountsL . accountsMapL)
    `Map.restrictKeys` creds

lookupRewards ::
  EraCertState era =>
  Set (Credential 'Staking) ->
  NewEpochState era ->
  Map (Credential 'Staking) Coin
lookupRewards creds =
  Map.map (fromCompact . (^. balanceAccountStateL)) . queryAccounts creds

lookupTotalRewards ::
  EraCertState era => Set (Credential 'Staking) -> NewEpochState era -> Maybe Coin
lookupTotalRewards creds nes =
  guard (Map.null credsRewards) $> fromCompact (foldMap (^. balanceAccountStateL) credsRewards)
  where
    credsRewards = queryAccounts creds nes

extractLedgerEvents ::
  [AuxLedgerEvent (ExtLedgerState (CardanoBlock c))] ->
  (forall era. EraApp era => ShelleyLedgerEvent era -> Maybe e) ->
  [e]
extractLedgerEvents extEvents handleEvent =
  mapMaybe applyTickLedgerEvent extEvents
  where
    applyTickLedgerEvent oneEraEvent =
      case fromTip $ getOneEraLedgerEvent oneEraEvent of
        TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (WrapLedgerEvent event)))))))) ->
          handleEvent event
        TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (WrapLedgerEvent event))))))) ->
          handleEvent event
        TS _ (TS _ (TS _ (TS _ (TS _ (TZ (WrapLedgerEvent event)))))) ->
          handleEvent event
        TS _ (TS _ (TS _ (TS _ (TZ (WrapLedgerEvent event))))) ->
          handleEvent event
        TS _ (TS _ (TS _ (TZ (WrapLedgerEvent event)))) ->
          handleEvent event
        TS _ (TS _ (TZ (WrapLedgerEvent event))) ->
          handleEvent event
        TS _ (TZ (WrapLedgerEvent event)) ->
          handleEvent event

extLedgerStateEpochNo :: ExtLedgerState (CardanoBlock c) mk -> EpochNo
extLedgerStateEpochNo =
  applyNewEpochState
    (EpochNo . Byron.getEpochNumber . Byron.currentEpoch . Byron.cvsUpdateState)
    (const nesEL)

tickedExtLedgerStateEpochNo ::
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  (TransitionInfo, EpochNo)
tickedExtLedgerStateEpochNo =
  applyTickedNewEpochState
    (\ti es -> (ti, EpochNo . Byron.getEpochNumber . Byron.currentEpoch $ Byron.cvsUpdateState es))
    (\ti es -> (ti, nesEL es))

detectNewRewards ::
  (HasLogFunc env, MonadReader env m, MonadIO m) =>
  Set (Credential 'Staking) ->
  EpochNo ->
  Map (Credential 'Staking) Coin ->
  Map (Credential 'Staking) Coin ->
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  m (EpochNo, Maybe (Map (Credential 'Staking) Coin, Map (Credential 'Staking) Coin))
detectNewRewards creds prevEpochNo prevRewards epochWithdrawals extLedgerState = do
  let (ti, curEpochNo) = tickedExtLedgerStateEpochNo extLedgerState
  unless (curEpochNo == prevEpochNo || curEpochNo == succ prevEpochNo) $
    logWarn $
      "Current epoch number: "
        <> displayShow curEpochNo
        <> " is too far apart from the previous one: "
        <> displayShow prevEpochNo
  case ti of
    TransitionUnknown _wo -> do
      -- logWarn $ "TransitionUnknown: " <> displayShow (curEpochNo, wo)
      pure (curEpochNo, Nothing)
    TransitionImpossible -> do
      logWarn $ "TransitionImpossible: " <> displayShow curEpochNo
      pure (curEpochNo, Nothing)
    TransitionKnown _en -> do
      -- unless (en == curEpochNo) $ do
      --   logWarn $ "Unexpected EpochNo mismatch: " <> displayShow (curEpochNo, en)
      -- unless (curEpochNo == prevEpochNo + 1) $ do
      --   logWarn $ "Unexpected EpochNo transition: " <> displayShow (curEpochNo, prevEpochNo + 1)
      -- rewards are payed out on the epoch boundary
      res <- forM (applyTickedNonByronNewEpochState (\_ -> lookupRewards creds) extLedgerState) $ \curRewards -> do
        let epochReceivedRewards =
              Map.merge
                Map.dropMissing -- credential was unregistered
                (Map.filterMissing $ \_ c -> c /= mempty) -- credential was registered. Discard zero rewards
                ( Map.zipWithMaybeMatched $ \cred cp cc -> do
                    -- Need to adjust for withdrawals
                    let cpw = maybe cp (cp <->) $ Map.lookup cred epochWithdrawals
                    guard (cpw /= cc) -- no change, means no new rewards
                    when (cc < cpw) $
                      error "New reward amounts can't be smaller than the previous ones"
                    let newRewardAmount = cc <-> cpw
                    guard (newRewardAmount /= mempty) -- Discard zero change to rewards
                    Just newRewardAmount -- new reward amount
                )
                prevRewards
                curRewards
        pure (curRewards, epochReceivedRewards)
      pure (curEpochNo, res)

data EpochBlockStats = EpochBlockStats
  { ebsEpochNo :: !EpochNo
  , ebsBlockStats :: !BlockStats
  }

instance ToNamedRecord EpochBlockStats where
  toNamedRecord EpochBlockStats{..} =
    let BlockStats{..} = ebsBlockStats
        mkLangFields fieldNames langStats =
          mconcat
            [ [ sizeFieldName .= (lsTotalSize <$> Map.lookup lang langStats)
              , countFieldName .= (lsTotalCount <$> Map.lookup lang langStats)
              ]
            | (lang, sizeFieldName, countFieldName) <- fieldNames
            ]
     in namedRecord $
          [ "EpochNo" .= unEpochNo ebsEpochNo
          , "BlocksSize" .= bsBlocksSize
          ]
            ++ mkLangFields scriptFieldNames bsScriptsStatsWits
            ++ mkLangFields scriptOutFieldNames esScriptsStatsOutScripts
            ++ mkLangFields scriptRefFieldNames esScriptsStatsRefScripts

mkLangFieldNames :: Field -> [AppLanguage] -> [(AppLanguage, Field, Field)]
mkLangFieldNames prefix langs =
  [ let langName = prefix <> toField (appLanguageToText lang)
     in (lang, langName <> "-size", langName <> "-count")
  | lang <- langs
  ]

scriptFieldNames :: [(AppLanguage, Field, Field)]
scriptFieldNames = mkLangFieldNames "" [minBound .. maxBound]

scriptOutFieldNames :: [(AppLanguage, Field, Field)]
scriptOutFieldNames = mkLangFieldNames "OutScript-" [minBound .. maxBound]

scriptRefFieldNames :: [(AppLanguage, Field, Field)]
scriptRefFieldNames = mkLangFieldNames "RefScript-" [minBound .. maxBound]

epochStatsToNamedCsv :: EpochStats -> NamedCsv
epochStatsToNamedCsv =
  NamedCsv blockStatsHeader . map (uncurry EpochBlockStats) . Map.toList . unEpochStats
  where
    blockStatsHeader =
      header $
        ["EpochNo", "BlocksSize"]
          ++ mconcat
            [ [sizeFieldName, countFieldName]
            | (_, sizeFieldName, countFieldName) <-
                scriptFieldNames ++ scriptOutFieldNames ++ scriptRefFieldNames
            ]

data BlockStats = BlockStats
  { bsBlocksSize :: !Int
  , bsScriptsStatsWits :: !(Map AppLanguage (ScriptsStats MaxScripts))
  , esScriptsStatsOutScripts :: !(Map AppLanguage (ScriptsStats MaxScripts))
  , esScriptsStatsRefScripts :: !(Map AppLanguage (ScriptsStats MaxScript))
  , esScriptsStatsAllRefScripts :: !(Map AppLanguage (ScriptsStats MaxScript))
  }
  deriving (Generic)

instance ToJSON BlockStats where
  toJSON = genericToJSON (defaultOptions{fieldLabelModifier = drop 2})

instance Semigroup BlockStats where
  es1 <> es2 =
    BlockStats
      { bsBlocksSize = bsBlocksSize es1 + bsBlocksSize es2
      , bsScriptsStatsWits =
          Map.unionWith (<>) (bsScriptsStatsWits es1) (bsScriptsStatsWits es2)
      , esScriptsStatsOutScripts =
          Map.unionWith (<>) (esScriptsStatsOutScripts es1) (esScriptsStatsOutScripts es2)
      , esScriptsStatsRefScripts =
          Map.unionWith (<>) (esScriptsStatsRefScripts es1) (esScriptsStatsRefScripts es2)
      , esScriptsStatsAllRefScripts =
          Map.unionWith
            (<>)
            (esScriptsStatsAllRefScripts es1)
            (esScriptsStatsAllRefScripts es2)
      }

instance Monoid BlockStats where
  mempty =
    BlockStats
      { bsBlocksSize = 0
      , bsScriptsStatsWits = mempty
      , esScriptsStatsOutScripts = mempty
      , esScriptsStatsRefScripts = mempty
      , esScriptsStatsAllRefScripts = mempty
      }

newtype EpochStats = EpochStats
  { unEpochStats :: Map EpochNo BlockStats
  }
  deriving (ToJSON)

instance Semigroup EpochStats where
  es1 <> es2 =
    EpochStats
      { unEpochStats =
          Map.unionWith (<>) (unEpochStats es1) (unEpochStats es2)
      }

instance Monoid EpochStats where
  mempty = EpochStats{unEpochStats = mempty}

toEpochStats :: EpochBlockStats -> EpochStats
toEpochStats EpochBlockStats{..} = EpochStats $ Map.singleton ebsEpochNo ebsBlockStats

instance Display EpochStats where
  display EpochStats{..} =
    mconcat
      [ "\n== " <> display epochNo <> ": ========\n" <> display blockStats
      | (epochNo, blockStats) <- Map.toList unEpochStats
      ]

instance Display BlockStats where
  display BlockStats{..} =
    "  Total size of blocks: "
      <> display bsBlocksSize
      <> mconcat
        [ "\n  Witnesses for " <> displayShow lang <> ":\n      " <> display langStats
        | (lang, langStats) <- Map.toList bsScriptsStatsWits
        ]
      <> mconcat
        [ "\n  Output scripts for " <> displayShow lang <> ":\n      " <> display langStats
        | (lang, langStats) <- Map.toList esScriptsStatsRefScripts
        ]
      <> mconcat
        [ "\n  Evaluated reference scripts for " <> displayShow lang <> ":\n      " <> display langStats
        | (lang, langStats) <- Map.toList esScriptsStatsRefScripts
        ]
      <> mconcat
        [ "\n  All reference scripts for " <> displayShow lang <> ":\n      " <> display langStats
        | (lang, langStats) <- Map.toList esScriptsStatsAllRefScripts
        ]

applyTickedNewEpochStateWithBlock ::
  (TransitionInfo -> ChainValidationState -> ByronBlock -> a) ->
  ( forall era.
    EraApp era =>
    TransitionInfo ->
    NewEpochState era ->
    ShelleyBlock (TPraos c) era ->
    a
  ) ->
  ( forall era.
    EraApp era =>
    TransitionInfo ->
    NewEpochState era ->
    ShelleyBlock (Praos c) era ->
    a
  ) ->
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  CardanoBlock c ->
  a
applyTickedNewEpochStateWithBlock fByron fTPraos fPraos tickedExtLedgerState block =
  case (tickedLedgerState tickedExtLedgerState, block) of
    (TickedLedgerStateByron ti ls, BlockByron blk) -> fByron ti (tickedByronLedgerState ls) blk
    (TickedLedgerStateShelley ti ls, BlockShelley blk) -> fTPraos ti (tickedShelleyLedgerState ls) blk
    (TickedLedgerStateAllegra ti ls, BlockAllegra blk) -> fTPraos ti (tickedShelleyLedgerState ls) blk
    (TickedLedgerStateMary ti ls, BlockMary blk) -> fTPraos ti (tickedShelleyLedgerState ls) blk
    (TickedLedgerStateAlonzo ti ls, BlockAlonzo blk) -> fTPraos ti (tickedShelleyLedgerState ls) blk
    (TickedLedgerStateBabbage ti ls, BlockBabbage blk) -> fPraos ti (tickedShelleyLedgerState ls) blk
    (TickedLedgerStateConway ti ls, BlockConway blk) -> fPraos ti (tickedShelleyLedgerState ls) blk
    _ -> error "Impossible combination of a ledegr state and a block"

applyTickedNewEpochStateWithTxs ::
  (ChainValidationState -> [B.ATxAux ByteString] -> a) ->
  (forall era. EraApp era => NewEpochState era -> [Tx era] -> a) ->
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  CardanoBlock c ->
  a
applyTickedNewEpochStateWithTxs fByron fShelleyOnwards =
  applyTickedNewEpochStateWithBlock
    (\_ti cvs -> fByron cvs . getByronTxs)
    (\_ti nes -> fShelleyOnwards nes . getShelleyOnwardsTxs)
    (\_ti nes -> fShelleyOnwards nes . getShelleyOnwardsTxs)

blockLanguageRefScriptsStats ::
  Ticked (ExtLedgerState (CardanoBlock c)) mk ->
  CardanoBlock c ->
  (Map AppLanguage (ScriptsStats MaxScript), Map AppLanguage (ScriptsStats MaxScript))
blockLanguageRefScriptsStats =
  applyTickedNewEpochStateWithTxs
    (\_ _ -> (Map.empty, Map.empty))
    ( \nes txs ->
        -- We need to account for interblock dependency of transactions. The simplest
        -- way to do that is just by adding all of the unspent outputs produced by the
        -- block. This is safe to do, because we know that all transactions are valid.
        let utxo = (nes ^. utxoL) <> foldMap utxoTx txs
            (usedRefScripts, allRefScripts) =
              unzip $ map (\tx -> refScriptsTxBody utxo (tx ^. bodyTxL)) txs
         in (calcStatsForAppScripts id usedRefScripts, calcStatsForAppScripts id allRefScripts)
    )
