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
{-# LANGUAGE TypeFamilies #-}

module Cardano.Streamer.LedgerState (
  encodeNewEpochState,
  applyNonByronNewEpochState,
  applyNewEpochState,
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
  pattern TickedLedgerStateAllegra,
  pattern TickedLedgerStateAlonzo,
  pattern TickedLedgerStateBabbage,
  pattern TickedLedgerStateConway,
  pattern TickedLedgerStateByron,
  pattern TickedLedgerStateMary,
  pattern TickedLedgerStateShelley,
) where

import Cardano.Chain.Block as Byron (ChainValidationState (cvsUpdateState))
import qualified Cardano.Chain.Slotting as Byron (EpochNumber (getEpochNumber))
import qualified Cardano.Chain.UTxO as B
import qualified Cardano.Chain.Update.Validation.Interface as Byron (State (currentEpoch))
import Cardano.Ledger.BaseTypes (EpochNo (..))
import Cardano.Ledger.Binary.Plain as Plain (Encoding, serialize, toCBOR)
import Cardano.Ledger.CertState
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.Shelley.LedgerState hiding (LedgerState)
import Cardano.Ledger.UMap as UM
import Cardano.Ledger.Val
import Cardano.Streamer.BlockInfo
import Cardano.Streamer.Common
import Cardano.Streamer.Ledger
import Data.Aeson (ToJSON (..), defaultOptions, fieldLabelModifier, genericToJSON)
import qualified Data.ByteString.Lazy as BSL
import Data.Csv (Field, ToNamedRecord (..), header, namedRecord, toField, (.=))
import qualified Data.Map.Merge.Strict as Map
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Byron.Ledger.Ledger
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Ledger.Extended
import Ouroboros.Consensus.Protocol.Praos
import Ouroboros.Consensus.Protocol.TPraos

-- import Ouroboros.Consensus.Shelley.Ledger (shelleyLedgerState)
import Ouroboros.Consensus.Shelley.Ledger.Block
import Ouroboros.Consensus.Shelley.Ledger.Ledger
import qualified RIO.Map as Map

import Data.SOP.BasicFunctors
import Data.SOP.Telescope

-- import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HardFork.Combinator.Ledger
import Ouroboros.Consensus.HardFork.Combinator.State.Types as State

encodeNewEpochState :: Crypto c => ExtLedgerState (CardanoBlock c) -> Encoding
encodeNewEpochState = applyNewEpochState toCBOR toCBOR

writeNewEpochState :: (Crypto c, MonadIO m) => FilePath -> ExtLedgerState (CardanoBlock c) -> m ()
writeNewEpochState fp = liftIO . BSL.writeFile fp . Plain.serialize . encodeNewEpochState

pattern TickedLedgerStateByron
  :: TransitionInfo
  -> Ticked (LedgerState ByronBlock)
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateByron ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TZ (State.Current{currentState = Comp st}))
      )

pattern TickedLedgerStateShelley
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (ShelleyEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateShelley ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TZ (State.Current{currentState = Comp st})))
      )

pattern TickedLedgerStateAllegra
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (AllegraEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateAllegra ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))
      )

pattern TickedLedgerStateMary
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (MaryEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateMary ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st})))))
      )

pattern TickedLedgerStateAlonzo
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (TPraos c) (AlonzoEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateAlonzo ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st}))))))
      )

pattern TickedLedgerStateBabbage
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (Praos c) (BabbageEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
pattern TickedLedgerStateBabbage ti st <-
  TickedHardForkLedgerState
    ti
    ( State.HardForkState
        (TS _ (TS _ (TS _ (TS _ (TS _ (TZ (State.Current{currentState = Comp st})))))))
      )

pattern TickedLedgerStateConway
  :: TransitionInfo
  -> Ticked (LedgerState (ShelleyBlock (Praos c) (ConwayEra c)))
  -> Ticked (LedgerState (CardanoBlock c))
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

applyNewEpochState
  :: Crypto c
  => (ChainValidationState -> a)
  -> (forall era. EraApp era c => NewEpochState era -> a)
  -> ExtLedgerState (CardanoBlock c)
  -> a
applyNewEpochState fByronBased fShelleyBased extLedgerState =
  case ledgerState extLedgerState of
    LedgerStateByron ls -> fByronBased (byronLedgerState ls)
    LedgerStateShelley ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateAllegra ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateMary ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateAlonzo ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateBabbage ls -> fShelleyBased (shelleyLedgerState ls)
    LedgerStateConway ls -> fShelleyBased (shelleyLedgerState ls)

applyNonByronNewEpochState
  :: Crypto c
  => (forall era. EraApp era c => NewEpochState era -> a)
  -> ExtLedgerState (CardanoBlock c)
  -> Maybe a
applyNonByronNewEpochState f = applyNewEpochState (const Nothing) (Just . f)

applyTickedNewEpochState
  :: Crypto c
  => (TransitionInfo -> ChainValidationState -> a)
  -> (forall era. EraApp era c => TransitionInfo -> NewEpochState era -> a)
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> a
applyTickedNewEpochState fByronBased fShelleyBased tickedExtLedgerState =
  case tickedLedgerState tickedExtLedgerState of
    TickedLedgerStateByron ti ls -> fByronBased ti (tickedByronLedgerState ls)
    TickedLedgerStateShelley ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateAllegra ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateMary ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateAlonzo ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateBabbage ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)
    TickedLedgerStateConway ti ls -> fShelleyBased ti (tickedShelleyLedgerState ls)

applyTickedNonByronNewEpochState
  :: Crypto c
  => (forall era. EraApp era c => TransitionInfo -> NewEpochState era -> a)
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> Maybe a
applyTickedNonByronNewEpochState f =
  applyTickedNewEpochState (\_ _ -> Nothing) (\ti -> Just . f ti)

lookupStakeCredentials
  :: Set (Credential 'Staking (EraCrypto era))
  -> NewEpochState era
  -> UM.StakeCredentials (EraCrypto era)
lookupStakeCredentials creds nes =
  let um = dsUnified (certDState (lsCertState (esLState (nesEs nes))))
   in UM.domRestrictedStakeCredentials creds um

lookupRewards
  :: Set (Credential 'Staking (EraCrypto era))
  -> NewEpochState era
  -> Map (Credential 'Staking (EraCrypto era)) Coin
lookupRewards creds nes = scRewards $ lookupStakeCredentials creds nes

lookupTotalRewards :: Set (Credential 'Staking (EraCrypto era)) -> NewEpochState era -> Maybe Coin
lookupTotalRewards creds nes = guard (Map.null credsRewards) >> pure (fold credsRewards)
  where
    credsRewards = lookupRewards creds nes

extLedgerStateEpochNo :: Crypto c => ExtLedgerState (CardanoBlock c) -> EpochNo
extLedgerStateEpochNo =
  applyNewEpochState
    (EpochNo . Byron.getEpochNumber . Byron.currentEpoch . Byron.cvsUpdateState)
    nesEL

tickedExtLedgerStateEpochNo
  :: Crypto c
  => Ticked (ExtLedgerState (CardanoBlock c))
  -> (TransitionInfo, EpochNo)
tickedExtLedgerStateEpochNo =
  applyTickedNewEpochState
    (\ti es -> (ti, EpochNo . Byron.getEpochNumber . Byron.currentEpoch $ Byron.cvsUpdateState es))
    (\ti es -> (ti, nesEL es))

detectNewRewards
  :: (Crypto c, HasLogFunc env, MonadReader env m, MonadIO m)
  => Set (Credential 'Staking c)
  -> EpochNo
  -> Map (Credential 'Staking c) Coin
  -> Map (Credential 'Staking c) Coin
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> m (EpochNo, Maybe (Map (Credential 'Staking c) Coin, Map (Credential 'Staking c) Coin))
detectNewRewards creds prevEpochNo prevRewards epochWithdrawals extLedgerState = do
  let (ti, curEpochNo) = tickedExtLedgerStateEpochNo extLedgerState
  unless (curEpochNo == prevEpochNo || curEpochNo == prevEpochNo + 1) $
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

applyTickedNewEpochStateWithBlock
  :: Crypto c
  => (TransitionInfo -> ChainValidationState -> ByronBlock -> a)
  -> ( forall era
        . EraApp era c
       => TransitionInfo
       -> NewEpochState era
       -> ShelleyBlock (TPraos (EraCrypto era)) era
       -> a
     )
  -> ( forall era
        . EraApp era c
       => TransitionInfo
       -> NewEpochState era
       -> ShelleyBlock (Praos (EraCrypto era)) era
       -> a
     )
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> CardanoBlock c
  -> a
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

applyTickedNewEpochStateWithTxs
  :: Crypto c
  => (ChainValidationState -> [B.ATxAux ByteString] -> a)
  -> (forall era. EraApp era c => NewEpochState era -> [Tx era] -> a)
  -> Ticked (ExtLedgerState (CardanoBlock c))
  -> CardanoBlock c
  -> a
applyTickedNewEpochStateWithTxs fByron fShelleyOnwards =
  applyTickedNewEpochStateWithBlock
    (\_ti cvs -> fByron cvs . getByronTxs)
    (\_ti nes -> fShelleyOnwards nes . getShelleyOnwardsTxs)
    (\_ti nes -> fShelleyOnwards nes . getShelleyOnwardsTxs)

blockLanguageRefScriptsStats
  :: Crypto c
  => Ticked (ExtLedgerState (CardanoBlock c))
  -> CardanoBlock c
  -> (Map AppLanguage (ScriptsStats MaxScript), Map AppLanguage (ScriptsStats MaxScript))
blockLanguageRefScriptsStats =
  applyTickedNewEpochStateWithTxs
    (\_ _ -> (Map.empty, Map.empty))
    ( \nes txs ->
        -- We need to account for interblock dependency of transactions. The simplest
        -- way to do that is just by adding all of the unspent outpus produced by the
        -- block. This is safe to do, because we know that all transactions are valid.
        let utxo =
              (nes ^. nesEsL . esLStateL . lsUTxOStateL . utxosUtxoL) <> foldMap utxoTx txs
            (usedRefScripts, allRefScripts) =
              unzip $ map (\tx -> refScriptsTxBody utxo (tx ^. bodyTxL)) txs
         in (calcStatsForAppScripts id usedRefScripts, calcStatsForAppScripts id allRefScripts)
    )
