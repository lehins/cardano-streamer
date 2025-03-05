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
  extLedgerStateEpochNo,
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
  pattern TickedLedgerStateAllegra,
  pattern TickedLedgerStateAlonzo,
  pattern TickedLedgerStateBabbage,
  pattern TickedLedgerStateConway,
  pattern TickedLedgerStateByron,
  pattern TickedLedgerStateMary,
  pattern TickedLedgerStateShelley,
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
import Cardano.Ledger.BaseTypes (BlockNo (..))
import Cardano.Ledger.Binary
import qualified Cardano.Ledger.Binary.Plain as Plain
import Cardano.Ledger.CertState
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Shelley.LedgerState hiding (LedgerState)
import Cardano.Ledger.State
import Cardano.Ledger.UMap as UM
import Cardano.Ledger.Val
import Cardano.Slotting.Slot
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.Ledger
import Data.Aeson (ToJSON (..), defaultOptions, fieldLabelModifier, genericToJSON)
import qualified Data.ByteString.Lazy as BSL
import Data.Csv (Field, ToNamedRecord (..), header, namedRecord, toField, (.=))
import qualified Data.Map.Merge.Strict as Map
import Data.Maybe (fromJust)
import Data.SOP.BasicFunctors
import Data.SOP.Telescope
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HardFork.Combinator.AcrossEras (OneEraTipInfo (..))
import Ouroboros.Consensus.HardFork.Combinator.Basics (LedgerState (HardForkLedgerState))
import Ouroboros.Consensus.HardFork.Combinator.Ledger
import Ouroboros.Consensus.HardFork.Combinator.State.Types as State
import Ouroboros.Consensus.HeaderValidation (
  AnnTip (..),
  HeaderState (..),
  TipInfoIsEBB (..),
 )
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Protocol.Praos
import Ouroboros.Consensus.Protocol.TPraos
import Ouroboros.Consensus.Shelley.Ledger.Block
import Ouroboros.Consensus.Shelley.Ledger.Ledger
import Ouroboros.Consensus.TypeFamilyWrappers (WrapTipInfo (..))
import qualified RIO.Map as Map

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

{-# COMPLETE TeleByron, TeleShelley, TeleAllegra, TeleMary, TeleAlonzo, TeleBabbage, TeleConway #-}

modifyLedgerState ::
  forall c.
  (ChainValidationState -> ChainValidationState) ->
  (forall era. EraApp era => NewEpochState era -> NewEpochState era) ->
  ExtLedgerState (CardanoBlock c) ->
  ExtLedgerState (CardanoBlock c)
modifyLedgerState fByronBased fShelleyBased extLedgerState =
  let modifyShelleyBased ::
        EraApp era =>
        ( Current LedgerState (ShelleyBlock (p c) era) ->
          Telescope (K Past) (Current LedgerState) (CardanoEras c)
        ) ->
        Current LedgerState (ShelleyBlock (p c) era) ->
        ExtLedgerState (CardanoBlock c)
      modifyShelleyBased tele current =
        let cs = currentState current
         in extLedgerState
              { ledgerState =
                  HardForkLedgerState $
                    State.HardForkState $
                      tele $
                        current
                          { currentState =
                              cs
                                { shelleyLedgerState = fShelleyBased (shelleyLedgerState cs)
                                }
                          }
              }
   in case ledgerState extLedgerState of
        HardForkLedgerState (HardForkState (TeleByron current)) ->
          let cs = currentState current
           in extLedgerState
                { ledgerState =
                    HardForkLedgerState $
                      State.HardForkState $
                        TeleByron $
                          current
                            { currentState =
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

encodeNewEpochState :: ExtLedgerState (CardanoBlock c) -> Plain.Encoding
encodeNewEpochState = applyNewEpochState toCBOR toCBOR

writeNewEpochState :: MonadIO m => FilePath -> ExtLedgerState (CardanoBlock c) -> m ()
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
  , EraCertState era
  , EraStake era
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
  Ticked (LedgerState ByronBlock) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateByron ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TZ (State.Current{currentState = Comp st}))
      )

pattern TickedLedgerStateShelley ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) ShelleyEra)) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateShelley ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TZ (State.Current{currentState = Comp st})))
      )

pattern TickedLedgerStateAllegra ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) AllegraEra)) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateAllegra ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))
      )

pattern TickedLedgerStateMary ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) MaryEra)) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateMary ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st})))))
      )

pattern TickedLedgerStateAlonzo ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (TPraos c) AlonzoEra)) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateAlonzo ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))))
      )

pattern TickedLedgerStateBabbage ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (Praos c) BabbageEra)) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateBabbage ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st})))))))
      )

pattern TickedLedgerStateConway ::
  TransitionInfo ->
  Ticked (LedgerState (ShelleyBlock (Praos c) ConwayEra)) ->
  Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateConway ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))))))
      )

{-# COMPLETE
  TickedLedgerStateByron
  , TickedLedgerStateShelley
  , TickedLedgerStateAllegra
  , TickedLedgerStateMary
  , TickedLedgerStateAlonzo
  , TickedLedgerStateBabbage
  , TickedLedgerStateConway
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
  ExtLedgerState (CardanoBlock StandardCrypto) -> Maybe Tip
tipFromExtLedgerState = tipFromHeaderState . headerState

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
  LedgerState (CardanoBlock StandardCrypto) -> Maybe Tip
tipFromLedgerState ledgerState =
  case ledgerState of
    LedgerStateByron ls -> toByronTip ls
    LedgerStateShelley ls -> toShelleyTip ls
    LedgerStateAllegra ls -> toShelleyTip ls
    LedgerStateMary ls -> toShelleyTip ls
    LedgerStateAlonzo ls -> toShelleyTip ls
    LedgerStateBabbage ls -> toShelleyTip ls
    LedgerStateConway ls -> toShelleyTip ls
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

applyNewEpochState ::
  (ChainValidationState -> a) ->
  (forall era. EraApp era => NewEpochState era -> a) ->
  ExtLedgerState (CardanoBlock c) ->
  a
applyNewEpochState fByronBased fShelleyBased extLedgerState =
  case ledgerState extLedgerState of
    LedgerStateByron ls -> fByronBased (byronLedgerState ls)
    LedgerStateShelley ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateAllegra ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateMary ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateAlonzo ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateBabbage ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateConway ls -> fShelleyBased (shelleyLedgerState ls)

applyNonByronNewEpochState ::
  (forall era. EraApp era => NewEpochState era -> a) ->
  ExtLedgerState (CardanoBlock c) ->
  Maybe a
applyNonByronNewEpochState f = applyNewEpochState (const Nothing) (Just . f)

applyTickedNewEpochState ::
  (TransitionInfo -> ChainValidationState -> a) ->
  (forall era. EraApp era => TransitionInfo -> NewEpochState era -> a) ->
  Ticked (ExtLedgerState (CardanoBlock c)) ->
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

applyTickedNonByronNewEpochState ::
  (forall era. EraApp era => TransitionInfo -> NewEpochState era -> a) ->
  Ticked (ExtLedgerState (CardanoBlock c)) ->
  Maybe a
applyTickedNonByronNewEpochState f =
  applyTickedNewEpochState (\_ _ -> Nothing) (\ti -> Just . f ti)

lookupStakeCredentials ::
  EraCertState era =>
  Set (Credential 'Staking) ->
  NewEpochState era ->
  UM.StakeCredentials
lookupStakeCredentials creds nes =
  let um = dsUnified (lsCertState (esLState (nesEs nes)) ^. certDStateL)
   in UM.domRestrictedStakeCredentials creds um

lookupRewards ::
  EraCertState era =>
  Set (Credential 'Staking) ->
  NewEpochState era ->
  Map (Credential 'Staking) Coin
lookupRewards creds nes = scRewards $ lookupStakeCredentials creds nes

lookupTotalRewards ::
  EraCertState era => Set (Credential 'Staking) -> NewEpochState era -> Maybe Coin
lookupTotalRewards creds nes = guard (Map.null credsRewards) >> pure (fold credsRewards)
  where
    credsRewards = lookupRewards creds nes

extLedgerStateEpochNo :: ExtLedgerState (CardanoBlock c) -> EpochNo
extLedgerStateEpochNo =
  applyNewEpochState
    (EpochNo . Byron.getEpochNumber . Byron.currentEpoch . Byron.cvsUpdateState)
    nesEL

tickedExtLedgerStateEpochNo ::
  Ticked (ExtLedgerState (CardanoBlock c)) ->
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
  Ticked (ExtLedgerState (CardanoBlock c)) ->
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
  Ticked (ExtLedgerState (CardanoBlock c)) ->
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
  Ticked (ExtLedgerState (CardanoBlock c)) ->
  CardanoBlock c ->
  a
applyTickedNewEpochStateWithTxs fByron fShelleyOnwards =
  applyTickedNewEpochStateWithBlock
    (\_ti cvs -> fByron cvs . getByronTxs)
    (\_ti nes -> fShelleyOnwards nes . getShelleyOnwardsTxs)
    (\_ti nes -> fShelleyOnwards nes . getShelleyOnwardsTxs)

blockLanguageRefScriptsStats ::
  Ticked (ExtLedgerState (CardanoBlock c)) ->
  CardanoBlock c ->
  (Map AppLanguage (ScriptsStats MaxScript), Map AppLanguage (ScriptsStats MaxScript))
blockLanguageRefScriptsStats =
  applyTickedNewEpochStateWithTxs
    (\_ _ -> (Map.empty, Map.empty))
    ( \nes txs ->
        -- We need to account for interblock dependency of transactions. The simplest
        -- way to do that is just by adding all of the unspent outpus produced by the
        -- block. This is safe to do, because we know that all transactions are valid.
        let utxo = (nes ^. utxoL) <> foldMap utxoTx txs
            (usedRefScripts, allRefScripts) =
              unzip $ map (\tx -> refScriptsTxBody utxo (tx ^. bodyTxL)) txs
         in (calcStatsForAppScripts id usedRefScripts, calcStatsForAppScripts id allRefScripts)
    )
