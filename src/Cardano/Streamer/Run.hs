{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Cardano.Streamer.Run (runApp) where

import Cardano.Ledger.Address
import Cardano.Ledger.BaseTypes (
  BlocksMade (..),
  BoundedRational (..),
  EpochNo (..),
  StrictMaybe (..),
  activeSlotCoeff,
  activeSlotVal,
  epochInfoPure,
  maxLovelaceSupply,
  securityParameter,
  )
import Cardano.Ledger.Slot (EpochSize (..), SlotNo (..), epochInfoSize)
import Cardano.Ledger.Coin (Coin (..), DeltaCoin (..))
import Cardano.Ledger.Compactible (fromCompact)
import Cardano.Ledger.Core (
  Reward (..),
  ppA0L,
  ppDG,
  ppMinPoolCostL,
  ppNOptL,
  ppProtocolVersionL,
  ppRhoL,
  ppTauL,
  )
import Cardano.Ledger.Conway.Governance (
  ConwayEraGov (committeeGovStateL, constitutionGovStateL),
  finishDRepPulser,
  newEpochStateDRepPulsingStateL,
  rsEnactStateL,
  )
import Cardano.Ledger.Conway.State (ConwayEraCertState, certVStateL, vsCommitteeStateL)
import Cardano.Ledger.Shelley.RewardUpdate (PulsingRewUpdate (..), RewardSnapShot (..), RewardUpdate (..))
import Cardano.Ledger.Hashes (unKeyHash)
import Cardano.Crypto.Hash.Class (hashToTextAsHex)
import Cardano.Ledger.Shelley.API (SnapShot (..), SnapShots (..))
import Cardano.Ledger.Shelley.LedgerState
import Cardano.Ledger.State (
  IndividualPoolStake (..),
  Obligations (..),
  PoolDistr (..),
  chainAccountStateL,
  casTreasuryL,
  casReservesL,
  individualPoolStake,
  individualTotalPoolStake,
  obligationCertState,
  obligationGovState,
  sumAllStake,
  sumObligation,
  unPoolDistr,
  )
import Cardano.Streamer.Benchmark
import Cardano.Streamer.Common
import Cardano.Streamer.Inspection
import Cardano.Streamer.LedgerState
import Cardano.Streamer.Producer
import Cardano.Streamer.ProtocolInfo
import Cardano.Protocol.Crypto (StandardCrypto)
import Ouroboros.Consensus.Cardano.Block (CardanoBlock)
import Ouroboros.Consensus.Config (TopLevelConfig)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import Cardano.Streamer.RTS
import Cardano.Streamer.Rewards
import Cardano.Ledger.Conway.Governance.DRepPulser (psDRepDistr)
import Cardano.Streamer.Storage (ledgerDbTipExtLedgerState)
import Conduit
import Control.ResourceRegistry (withRegistry)
import Criterion.Measurement (initializeTime)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Lazy.Char8 as BSL
import Data.Char (toLower)
import qualified Data.List.NonEmpty as NE
import Data.Functor.Identity (runIdentity)
import Control.Monad.Trans.Reader (runReaderT)
import Data.Ratio (denominator, numerator, (%))
import qualified Data.Map.Strict as Map
import Lens.Micro ((^.))
import Ouroboros.Consensus.Ledger.Extended (ExtLedgerState, ledgerState)
import Ouroboros.Consensus.Shelley.Ledger.Ledger (shelleyLedgerState)
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..))
import RIO.Directory (createDirectoryIfMissing, doesDirectoryExist, doesPathExist, listDirectory)
import RIO.File (withBinaryFileDurable)
import RIO.FilePath
import RIO.List as List
import qualified RIO.Set as Set
import qualified RIO.Text as T

replayChain :: RIO App ()
replayChain =
  runConduit $ sourceBlocksWithInspector_ (SlotInspector noInspection) .| sinkNull

replayBenchmarkReport :: RIO App ()
replayBenchmarkReport = do
  report <-
    runConduit $
      sourceBlocksWithInspector (GetPure ()) (SlotInspector benchmarkInspection) .| calcStatsReport
  writeReport "Benchmark" report

replayEpochStats :: RIO App ()
replayEpochStats = do
  epochStats <-
    runConduit $
      sourceBlocksWithInspector (GetPure ()) (SlotInspector epochBlockStatsInspection)
        .| foldMapC toEpochStats
  writeReport "EpochStats" epochStats
  writeNamedCsv "EpochStats" (epochStatsToNamedCsv epochStats)
  logInfo $ "Final summary: \n    " <> display (fold $ unEpochStats epochStats)

-- | Encode a Rational exactly as {"numerator": n, "denominator": d}.
rationalToJson :: Rational -> Aeson.Value
rationalToJson r =
  Aeson.object
    [ "numerator" Aeson..= numerator r
    , "denominator" Aeson..= denominator r
    ]

-- | Build the JSON snapshot for a given ledger state.
-- Returns Nothing for Byron.
-- Returns Just (fullJson, rupdNext) where rupdNext should be threaded to the
-- next epoch's call as mRupdApplied (it is the reward update that will be
-- applied at that epoch boundary).
buildSnapshotJson ::
  TopLevelConfig (CardanoBlock StandardCrypto) ->
  Maybe Aeson.Value ->
  ExtLedgerState (CardanoBlock StandardCrypto) mk ->
  Maybe (Aeson.Value, Aeson.Value)
buildSnapshotJson topLevelConfig mRupdApplied extLedgerState =
  let eraName = show $ extLedgerStateCardanoEra extLedgerState
      mGlobals = globalsFromLedgerConfig (extLedgerStateCardanoEra extLedgerState) extLedgerState topLevelConfig
      mConwayGov = applyConwayNewEpochState extractConwayGovData extLedgerState
      mEpochNonce = extLedgerStateEpochNonce extLedgerState
   in applyNonByronNewEpochState (extractSnapshotData eraName mGlobals mConwayGov mRupdApplied mEpochNonce) extLedgerState
  where
    extractConwayGovData ::
      (ConwayEraGov era, ConwayEraCertState era) => NewEpochState era -> Aeson.Value
    extractConwayGovData nes =
      let (snap, ratifyState) = finishDRepPulser (nes ^. newEpochStateDRepPulsingStateL)
          drepDistr = Map.map fromCompact (psDRepDistr snap)
          committee = nes ^. newEpochStateGovStateL . committeeGovStateL
          constitution = nes ^. newEpochStateGovStateL . constitutionGovStateL
          committeeState =
            nes ^. nesEpochStateL . esLStateL . lsCertStateL . certVStateL . vsCommitteeStateL
          nextEnactState = ratifyState ^. rsEnactStateL
       in Aeson.object
            [ "drepDistr" Aeson..= drepDistr
            , "committee" Aeson..= committee
            , "constitution" Aeson..= constitution
            , "committeeState" Aeson..= committeeState
            , "nextEnactState" Aeson..= nextEnactState
            ]

    -- Returns (fullJson, rupdData) where rupdData is threaded to the next epoch.
    extractSnapshotData eraName mGlobals mConwayGov mPrevRupd mEpochNonce nes =
      let epochNum = case nesEL nes of EpochNo n -> n
          epochState = nesEs nes
          poolDistr = nesPd nes
          treasuryAmt = unCoin $ epochState ^. chainAccountStateL . casTreasuryL
          reservesAmt = unCoin $ epochState ^. chainAccountStateL . casReservesL

          -- Extract all 3 snapshots (mark, set, go) and fees
          SnapShots {ssStakeMark = markSnap, ssStakeSet = setSnap, ssStakeGo = goSnap, ssFee = feeCoin} = epochState ^. esSnapshotsL
          fees = unCoin feeCoin

          -- Extract deposit obligations
          obligations =
            obligationCertState (epochState ^. esLStateL . lsCertStateL)
              <> obligationGovState (nes ^. newEpochStateGovStateL)

          sumRs m =
            sum
              [ unCoin (rewardAmount r)
              | rs_set <- Map.elems m
              , r <- Set.toList rs_set
              ] :: Integer

          fromPulsing globals rewsnap@RewardSnapShot {..} pulser =
            let Coin rPot' = rewFees <> rewDeltaR1
                (RewardUpdate {rs = forcedRs}, _) =
                  runIdentity $ runReaderT (completeRupd (Pulsing rewsnap pulser)) globals
                totalDistributed = sumRs forcedRs
                deltaR2 = unCoin rewR - totalDistributed
             in Aeson.object
                  [ "deltaR1" Aeson..= unCoin rewDeltaR1
                  , "deltaR2" Aeson..= deltaR2
                  , "deltaT1" Aeson..= unCoin rewDeltaT1
                  , "rPot" Aeson..= rPot'
                  , "rewardPot" Aeson..= unCoin rewR
                  , "totalDistributed" Aeson..= totalDistributed
                  ]

          rupdData =
            case (nesRu nes, mGlobals) of
              (SJust (Complete RewardUpdate {..}), _) ->
                let DeltaCoin deltaT1 = deltaT
                    DeltaCoin deltaRCombined = deltaR
                 in Aeson.object
                      [ "deltaT1" Aeson..= deltaT1
                      , "deltaR" Aeson..= deltaRCombined
                      , "totalDistributed" Aeson..= sumRs rs
                      ]
              (_, Nothing) -> Aeson.Null
              (SJust (Pulsing rewsnap pulser), Just globals) ->
                fromPulsing globals rewsnap pulser
              (SNothing, Just globals) ->
                let pulsing =
                      startStep
                        (epochInfoSize (epochInfoPure globals) (nesEL nes))
                        (nesBprev nes)
                        (nesEs nes)
                        (Coin $ fromIntegral $ maxLovelaceSupply globals)
                        (activeSlotCoeff globals)
                        (securityParameter globals)
                 in case pulsing of
                      Pulsing rewsnap pulser -> fromPulsing globals rewsnap pulser
                      Complete RewardUpdate {..} ->
                        Aeson.object ["totalDistributed" Aeson..= sumRs rs]

          -- Eta: performance multiplier. Computed the same way the ledger does
          -- in startStep: if d >= 0.8 then 1, otherwise blocksMade/expectedBlocks.
          -- (RewardSnapShot does not store eta, so we derive it from nesBprev.)
          mEta = mGlobals <&> \globals ->
            let d = unboundRational (pr ^. ppDG)
                EpochSize slots = epochInfoSize (epochInfoPure globals) (nesEL nes)
                n = floor $ (1 - d) * unboundRational (activeSlotVal (activeSlotCoeff globals)) * fromIntegral slots :: Integer
                BlocksMade bm = nesBprev nes
                blocksMade = fromIntegral $ Map.foldl' (+) 0 bm :: Integer
             in if d >= 0.8 || n == 0
                  then 1 :: Rational
                  else blocksMade % n

          -- Protocol parameters: encoded as exact rationals, not Double
          pr = epochState ^. prevPParamsEpochStateL
          protoParams =
            Aeson.object
              [ "rho" Aeson..= rationalToJson (unboundRational (pr ^. ppRhoL))
              , "tau" Aeson..= rationalToJson (unboundRational (pr ^. ppTauL))
              , "d" Aeson..= rationalToJson (unboundRational (pr ^. ppDG))
              , "a0" Aeson..= rationalToJson (unboundRational (pr ^. ppA0L))
              , "nOpt" Aeson..= (pr ^. ppNOptL)
              , "minPoolCost" Aeson..= unCoin (pr ^. ppMinPoolCostL)
              , "protocolVersion" Aeson..= (pr ^. ppProtocolVersionL)
              ]

          -- Circulation and active stake
          totalStake = case mGlobals of
            Nothing -> Nothing
            Just globals ->
              Just $ fromIntegral (maxLovelaceSupply globals) - reservesAmt :: Maybe Integer
          activeStake = unCoin $ sumAllStake (ssStake goSnap)

          -- Pending MIR transfers (Shelley–Babbage; always empty in Conway+)
          instantaneousRewards =
            epochState ^. esLStateL . lsCertStateL . certDStateL . dsIRewardsL

          -- expectedBlocks is derived from genesis/pparams; eta comes from the pulser
          mExpectedBlocks = mGlobals <&> \globals ->
            let d = unboundRational (pr ^. ppDG)
                EpochSize slots = epochInfoSize (epochInfoPure globals) (nesEL nes)
                asc = activeSlotCoeff globals
             in floor $ (1 - d) * unboundRational (activeSlotVal asc) * fromIntegral slots :: Integer

          -- Pool distribution: stake as exact rational + exact lovelace count.
          -- The fractions in PoolDistr sum to exactly 1 by ledger construction,
          -- so stakePercent is simply stakeRational * 100.
          poolMap = unPoolDistr poolDistr
          poolCount = Map.size poolMap
          poolEntries = map mkPoolEntry (Map.toList poolMap)
            where
              mkPoolEntry (pid, poolData) =
                let stakeRational = individualPoolStake poolData
                    stakeLovelace = unCoin $ fromCompact (individualTotalPoolStake poolData)
                    stakePercent = fromRational (stakeRational * 100) :: Double
                 in Aeson.object
                      [ "poolId" Aeson..= hashToTextAsHex (unKeyHash pid)
                      , "stake" Aeson..= rationalToJson stakeRational
                      , "stakeLovelace" Aeson..= (stakeLovelace :: Integer)
                      , "stakePercent" Aeson..= stakePercent
                      ]

          snapshotInfo name mBlocks snap =
            Aeson.object $
              [ "name" Aeson..= (name :: String)
              , "stake" Aeson..= ssStake snap
              , "delegations" Aeson..= ssDelegations snap
              , "poolParams" Aeson..= ssPoolParams snap
              ]
                ++ ["blocks" Aeson..= b | Just b <- [mBlocks]]

          json =
            Aeson.object
              [ "epoch" Aeson..= (fromIntegral epochNum :: Integer)
              , "snapshotEraName" Aeson..= eraName
              , "epochNonce" Aeson..= mEpochNonce
              , "protocolParams" Aeson..= protoParams
              , "totalStake" Aeson..= totalStake
              , "activeStake" Aeson..= activeStake
              , "eta" Aeson..= fmap rationalToJson mEta
              , "expectedBlocks" Aeson..= mExpectedBlocks
              , "rupdNext" Aeson..= rupdData
              , "rupdApplied" Aeson..= mPrevRupd
              , "treasury" Aeson..= treasuryAmt
              , "reserves" Aeson..= reservesAmt
              , "totalPools" Aeson..= poolCount
              , "poolDistribution" Aeson..= poolEntries
              , "epochFees" Aeson..= fees
              , "deposits"
                  Aeson..= Aeson.object
                    [ "stakeKey" Aeson..= unCoin (oblStake obligations)
                    , "pool" Aeson..= unCoin (oblPool obligations)
                    , "dRep" Aeson..= unCoin (oblDRep obligations)
                    , "proposal" Aeson..= unCoin (oblProposal obligations)
                    , "total" Aeson..= unCoin (sumObligation obligations)
                    ]
              , "instantaneousRewards" Aeson..= instantaneousRewards
              , "conwayGov" Aeson..= mConwayGov
              , "snapshots"
                  Aeson..= Aeson.object
                    [ "mark" Aeson..= snapshotInfo "mark" (Just (nesBcur nes)) markSnap
                    , "set" Aeson..= snapshotInfo "set" (Nothing :: Maybe BlocksMade) setSnap
                    , "go" Aeson..= snapshotInfo "go" (Just (nesBprev nes)) goSnap
                    ]
              ]
       in (json, rupdData)

dumpLedgerSnapshot :: RIO App ()
dumpLedgerSnapshot = do
  extLedgerState <- ledgerDbTipExtLedgerState
  app <- ask
  case buildSnapshotJson (pInfoConfig (dsAppProtocolInfo app)) Nothing extLedgerState of
    Just (snapshotData, _) -> liftIO $ BSL.putStrLn $ Aeson.encode snapshotData
    Nothing -> logError "Cannot dump Byron era snapshot"

dumpEpochSnapshots :: RIO App ()
dumpEpochSnapshots = do
  app <- ask
  outDir <-
    maybe (throwString "--out-dir is required for dump-epoch-snapshots") pure
      =<< asks dsAppOutDir
  prevRupdRef <- newIORef Nothing
  let topLevelConfig = pInfoConfig (dsAppProtocolInfo app)
      snapshotInspection =
        noInspection
          { siFinal = \swb _ _ _ _ _ ->
              when (isFirstSlotOfNewEpoch swb) $ do
                prevRupd <- readIORef prevRupdRef
                -- We use the post-block state (DiffMK) rather than the bare
                -- epoch-boundary state. This is safe because buildSnapshotJson reads
                -- exclusively from NewEpochState fields (epoch number, treasury,
                -- reserves, snapshots, pool distribution, protocol params, reward
                -- update) — none of which are part of the UTxO table, so the
                -- unapplied DiffMK diffs are irrelevant.
                -- INVARIANT: do not add snapshot fields that read from the UTxO
                -- (esUTxOState / utxosUtxo) without switching to a ValuesMK state.
                let mResult = buildSnapshotJson topLevelConfig prevRupd (swbNewExtLedgerState swb)
                forM_ mResult $ \(json, rupdNext) -> do
                  writeIORef prevRupdRef (Just rupdNext)
                  let epochStr = show (unEpochNo (swbEpochNo swb))
                      slotStr = show (unSlotNo (swbSlotNo swb))
                      fp = outDir </> epochStr <> "-" <> slotStr <> ".json"
                  liftIO $ BSL.writeFile fp (Aeson.encode json)
                  logInfo $
                    "Dumped epoch "
                      <> display (swbEpochNo swb)
                      <> " snapshot to: "
                      <> display (T.pack fp)
          }
  runConduit $ sourceBlocksWithInspector_ (SlotInspector snapshotInspection) .| sinkNull

replayRewards :: NE.NonEmpty RewardAccount -> RIO App ()
replayRewards accounts = do
  mOutDir <- dsAppOutDir <$> ask
  case mOutDir of
    Nothing ->
      logError "Output directory is required for exporting rewards"
    Just outDir -> do
      let filePaths =
            [ ( raCredential account,
                outDir </> T.unpack (formatRewardAccount account) <.> "csv"
              )
              | account <- NE.toList accounts
            ]
          withRewardsFiles action = go [] filePaths
            where
              go hdls [] = action $ Map.fromList hdls
              go hdls ((ra, fp) : fps) =
                withBinaryFileDurable fp WriteMode $ \hdl -> do
                  logInfo $ "Opened file for exporting rewards " <> displayShow fp
                  go ((ra, hdl) : hdls) fps
          transformAndFilterRewards handles rd = do
            let filteredRewardsWithHandles = Map.intersectionWith (,) handles (rdRewards rd)
            guard (not (Map.null filteredRewardsWithHandles))
            Just $ map (\(h, c) -> (h, rd {rdRewards = c})) $ Map.elems filteredRewardsWithHandles
      withRewardsFiles $ \rewardHandles -> do
        writeRewardsHeaders rewardHandles
        runConduit $
          sourceBlocksWithInspector (GetPure ()) (SlotInspector rewardsInspection)
            .| concatMapC (>>= transformAndFilterRewards rewardHandles)
            .| mapM_C (mapM_ (uncurry writeRewardDistribution))

runApp :: Opts -> IO ()
runApp Opts {..} = do
  -- Consensus code will initialize the chain directory if it doesn't exist, which makes
  -- no sense for a tool that is suppose to be read only.
  unlessM (doesDirectoryExist oChainDir) $ do
    throwString $ "Chain directory does not exist: " <> oChainDir
  whenM (null <$> listDirectory oChainDir) $ do
    throwString $ "Chain directory is empty: " <> oChainDir
  logOpts <- logOptionsHandle stderr oVerbose
  withLogFunc (setLogMinLevel oLogLevel $ setLogUseLoc oDebug logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let (writeBlockSlots, writeBlockHashes) =
            bimap Set.fromList Set.fromList $
              partitionEithers $
                map unBlockHashOrSlotNo oWriteBlocks
          appConf =
            AppConfig
              { appConfChainDir = oChainDir,
                appConfFilePath = oConfigFilePath,
                appConfReadDiskSnapshot =
                  DiskSnapshot <$> oReadSnapShotSlotNumber <*> pure oSnapShotSuffix,
                appConfWriteDiskSnapshots =
                  DiskSnapshot <$> oWriteSnapShotSlotNumbers <*> pure oSnapShotSuffix,
                appConfStopSlotNumber = oStopSlotNumber,
                appConfValidationMode = oValidationMode,
                appConfWriteBlocksSlotNoSet = writeBlockSlots,
                appConfWriteBlocksBlockHashSet = writeBlockHashes,
                appConfLogFunc = logFunc,
                appConfRegistry = registry
              }
      initializeTime
      void $ runRIO appConf $ runDbStreamerApp $ do
        forM_ oOutDir (createMissingDirectory "output")
        rtsStatsFilePathMaybe <-
          forM oRTSStatsFilePath $ \rtsStatsFilePath -> do
            -- We need to fail when stats are not enabled before we start creating files
            checkRTSStatsEnabled
            fullPath <-
              if isAbsolute rtsStatsFilePath
                then
                  pure rtsStatsFilePath
                else case oOutDir of
                  Nothing ->
                    throwString $
                      "Supplied a relative path for RTS stats: '"
                        <> rtsStatsFilePath
                        <> "' without supplying the OUT_DIR"
                  Just outDir -> pure $ outDir </> rtsStatsFilePath
            createMissingDirectory "RTS Stats File" (dropFileName fullPath)
            whenM (doesPathExist fullPath) $
              throwString $
                "Can't use an existing file for writing RTS stats: " <> fullPath
            let ext = toLower <$> takeExtension fullPath
            unless (ext == ".csv") $
              logWarn $
                "Expected a file path for a CSV file, but "
                  <> displayShow fullPath
                  <> " has an unexpected extension: "
                  <> displayShow ext
            pure fullPath
        unless (null writeBlockSlots) $
          logInfo $
            "Will try to dump blocks if they exist at slots: "
              <> mconcat (intersperse "," (map display (Set.toList writeBlockSlots)))
        unless (null writeBlockHashes) $
          logInfo $
            "Will try to dump blocks with hashes if they exist: "
              <> mconcat
                (intersperse "," (map display (Set.toList writeBlockHashes)))
        app <- ask
        withMaybeFile rtsStatsFilePathMaybe $ \rtsStatsHandle ->
          runRIO (app {dsAppOutDir = oOutDir, dsAppRTSStatsHandle = rtsStatsHandle}) $ do
            logInfo $ "Starting to " <> display oCommand
            writeStreamerHeader
            case oCommand of
              Replay -> replayChain
              Benchmark -> replayBenchmarkReport
              Stats -> replayEpochStats
              ComputeRewards accountIds -> replayRewards accountIds
              DumpSnapshot -> dumpLedgerSnapshot
              DumpEpochSnapshots -> dumpEpochSnapshots
  where
    withMaybeFile mFilePath action =
      case mFilePath of
        Nothing -> action Nothing
        Just fp -> withBinaryFileDurable fp WriteMode (action . Just)
    createMissingDirectory name dir =
      unlessM (doesDirectoryExist dir) $ do
        logInfo $ "Creating " <> name <> " directory: " <> displayShow dir
        createDirectoryIfMissing True dir

-- -- TxOuts:
-- total <- runRIO (app{dsAppOutDir = mOutDir}) $ countTxOuts initLedger
-- logInfo $ "Total TxOuts: " <> displayShow total
-- runRIO (app{dsAppOutDir = mOutDir}) $ revalidateWriteNewEpochState initLedger
