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
import Cardano.Ledger.Slot (EpochSize (..), epochInfoSize)
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
import Cardano.Ledger.PoolDistr (PoolDistr (..))
import qualified Cardano.Ledger.PoolDistr as PD
import Cardano.Ledger.Shelley.API (SnapShot (..), SnapShots (..), Stake (..))
import Cardano.Ledger.Shelley.LedgerState
import Cardano.Ledger.State (Obligations (..), obligationCertState, obligationGovState, sumAllStake, sumObligation)
import qualified Data.VMap as VMap
import Cardano.Streamer.Benchmark
import Cardano.Streamer.Common
import Cardano.Streamer.Inspection
import Cardano.Streamer.LedgerState
import Cardano.Streamer.Producer
import Cardano.Streamer.ProtocolInfo
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
import Control.Monad.Trans.Reader (runReaderT)
import Data.Functor.Identity (runIdentity)
import qualified Data.Map.Strict as Map
import Lens.Micro ((^.))
import Ouroboros.Consensus.Ledger.Extended (ledgerState)
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

dumpLedgerSnapshot :: RIO App ()
dumpLedgerSnapshot = do
  extLedgerState <- ledgerDbTipExtLedgerState
  app <- ask
  let eraName = show $ extLedgerStateCardanoEra extLedgerState
      mGlobals =
        globalsFromLedgerConfig
          (extLedgerStateCardanoEra extLedgerState)
          extLedgerState
          (pInfoConfig (dsAppProtocolInfo app))
      mConwayGov = applyConwayNewEpochState extractConwayGovData extLedgerState
      maybeData = applyNonByronNewEpochState (extractSnapshotData eraName mGlobals mConwayGov) extLedgerState
  case maybeData of
    Just snapshotData -> liftIO $ BSL.putStrLn $ Aeson.encode snapshotData
    Nothing -> logError "Cannot dump Byron era snapshot"
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

    extractSnapshotData eraName mGlobals mConwayGov nes =
      let epochNum = case nesEL nes of EpochNo n -> n
          epochState = nesEs nes
          poolDistr = nesPd nes
          accountState = epochState ^. esAccountStateL
          treasuryAmt = unCoin $ accountState ^. asTreasuryL
          reservesAmt = unCoin $ accountState ^. asReservesL

          -- Extract all 3 snapshots (mark, set, go) and fees
          SnapShots {ssStakeMark = markSnap, ssStakeSet = setSnap, ssStakeGo = goSnap, ssFee = feeCoin} = epochState ^. esSnapshotsL
          fees = unCoin feeCoin

          -- Extract deposit obligations
          obligations = obligationCertState (epochState ^. esLStateL . lsCertStateL)
                     <> obligationGovState (nes ^. newEpochStateGovStateL)

          -- Extract reward update data, forcing the pulser to completion if needed.
          -- For SNothing (before stability point), synthesize via startStep.
          rupdData =
            let sumRs m =
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
             in case (nesRu nes, mGlobals) of
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

          -- Protocol parameters relevant to reward/treasury calculation
          pr = epochState ^. prevPParamsEpochStateL
          protoParams =
            Aeson.object
              [ "rho" Aeson..= (fromRational (unboundRational (pr ^. ppRhoL)) :: Double)
              , "tau" Aeson..= (fromRational (unboundRational (pr ^. ppTauL)) :: Double)
              , "d" Aeson..= (fromRational (unboundRational (pr ^. ppDG)) :: Double)
              , "a0" Aeson..= (fromRational (unboundRational (pr ^. ppA0L)) :: Double)
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

          -- Eta (performance multiplier) and expected blocks
          etaAndExpected = mGlobals <&> \globals ->
            let d = unboundRational (pr ^. ppDG)
                EpochSize slots = epochInfoSize (epochInfoPure globals) (nesEL nes)
                asc = activeSlotCoeff globals
                expectedBlocks =
                  floor $ (1 - d) * unboundRational (activeSlotVal asc) * fromIntegral slots :: Integer
                blocksMadeCount =
                  fromIntegral $ Map.foldr (+) 0 (unBlocksMade (nesBprev nes)) :: Integer
                eta :: Double
                eta
                  | d >= 0.8 = 1.0
                  | expectedBlocks == 0 = 1.0
                  | otherwise = fromIntegral blocksMadeCount / fromIntegral expectedBlocks
             in (eta, expectedBlocks)

          poolMap = PD.unPoolDistr poolDistr
          totalStakeRational = sum [PD.individualPoolStake pd | pd <- Map.elems poolMap]
          poolCount = Map.size poolMap
          poolEntries = map mkPoolEntry (Map.toList poolMap)
            where
              mkPoolEntry (pid, poolData) =
                let stakeRational = PD.individualPoolStake poolData
                    percent =
                      if totalStakeRational > 0
                        then fromRational (stakeRational / totalStakeRational * 100) :: Double
                        else 0
                 in Aeson.object
                      [ "poolId" Aeson..= hashToTextAsHex (unKeyHash pid),
                        "stake" Aeson..= (fromRational stakeRational :: Double),
                        "stakePercent" Aeson..= percent
                      ]

          -- Helper to extract full snapshot data for comparison
          snapshotInfo name mBlocks snap =
            Aeson.object $
              [ "name" Aeson..= (name :: String)
              , "stake" Aeson..= ssStake snap
              , "delegations" Aeson..= ssDelegations snap
              , "poolParams" Aeson..= ssPoolParams snap
              ]
                ++ ["blocks" Aeson..= b | Just b <- [mBlocks]]

       in Aeson.object
            [ "epoch" Aeson..= (fromIntegral epochNum :: Integer),
              "snapshotEraName" Aeson..= eraName,
              "protocolParams" Aeson..= protoParams,
              "totalStake" Aeson..= totalStake,
              "activeStake" Aeson..= activeStake,
              "eta" Aeson..= fmap fst etaAndExpected,
              "expectedBlocks" Aeson..= fmap snd etaAndExpected,
              "rupdNext" Aeson..= rupdData,
              "treasury" Aeson..= treasuryAmt,
              "reserves" Aeson..= reservesAmt,
              "totalPools" Aeson..= poolCount,
              "poolDistribution" Aeson..= poolEntries,
              "epochFees" Aeson..= fees,
              "deposits" Aeson..= Aeson.object
                [ "stakeKey" Aeson..= unCoin (oblStake obligations)
                , "pool" Aeson..= unCoin (oblPool obligations)
                , "dRep" Aeson..= unCoin (oblDRep obligations)
                , "proposal" Aeson..= unCoin (oblProposal obligations)
                , "total" Aeson..= unCoin (sumObligation obligations)
                ],
              "instantaneousRewards" Aeson..= instantaneousRewards,
              "conwayGov" Aeson..= mConwayGov,
              "snapshots" Aeson..= Aeson.object
                [ "mark" Aeson..= snapshotInfo "mark" (Just (nesBcur nes)) markSnap
                , "set" Aeson..= snapshotInfo "set" (Nothing :: Maybe BlocksMade) setSnap
                , "go" Aeson..= snapshotInfo "go" (Just (nesBprev nes)) goSnap
                ]
            ]

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
