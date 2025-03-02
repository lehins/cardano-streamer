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
import Cardano.Streamer.Benchmark
import Cardano.Streamer.Common
import Cardano.Streamer.Inspection
import Cardano.Streamer.LedgerState
import Cardano.Streamer.Producer
import Cardano.Streamer.ProtocolInfo
import Cardano.Streamer.RTS
import Cardano.Streamer.Rewards
import Conduit
import Control.ResourceRegistry (withRegistry)
import Criterion.Measurement (initializeTime)
import Data.Char (toLower)
import qualified Data.List.NonEmpty as NE
import qualified Data.Map.Strict as Map
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

replayRewards :: NE.NonEmpty RewardAccount -> RIO App ()
replayRewards accounts = do
  mOutDir <- dsAppOutDir <$> ask
  case mOutDir of
    Nothing ->
      logError "Output directory is required for exporting rewards"
    Just outDir -> do
      let filePaths =
            [ ( raCredential account
              , outDir </> T.unpack (formatRewardAccount account) <.> "csv"
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
            Just $ map (\(h, c) -> (h, rd{rdRewards = c})) $ Map.elems filteredRewardsWithHandles
      withRewardsFiles $ \rewardHandles -> do
        writeRewardsHeaders rewardHandles
        runConduit $
          sourceBlocksWithInspector (GetPure ()) (SlotInspector rewardsInspection)
            .| concatMapC (>>= transformAndFilterRewards rewardHandles)
            .| mapM_C (mapM_ (uncurry writeRewardDistribution))

runApp :: Opts -> IO ()
runApp Opts{..} = do
  -- Consensus code will initialize the chain directory if it doesn't exist, which makes
  -- no sense for a tool that is suppose to be read only.
  unlessM (doesDirectoryExist oChainDir) $ do
    throwString $ "Chain directory does not exist: " <> oChainDir
  whenM (null <$> listDirectory oChainDir) $ do
    throwString $ "Chain directory is empty: " <> oChainDir
  logOpts <- logOptionsHandle stdout oVerbose
  withLogFunc (setLogMinLevel oLogLevel $ setLogUseLoc oDebug logOpts) $ \logFunc -> do
    withRegistry $ \registry -> do
      let (writeBlockSlots, writeBlockHashes) =
            bimap Set.fromList Set.fromList $
              partitionEithers $
                map unBlockHashOrSlotNo oWriteBlocks
          appConf =
            AppConfig
              { appConfChainDir = oChainDir
              , appConfFilePath = oConfigFilePath
              , appConfReadDiskSnapshot =
                  DiskSnapshot <$> oReadSnapShotSlotNumber <*> pure oSnapShotSuffix
              , appConfWriteDiskSnapshots =
                  DiskSnapshot <$> oWriteSnapShotSlotNumbers <*> pure oSnapShotSuffix
              , appConfStopSlotNumber = oStopSlotNumber
              , appConfValidationMode = oValidationMode
              , appConfWriteBlocksSlotNoSet = writeBlockSlots
              , appConfWriteBlocksBlockHashSet = writeBlockHashes
              , appConfLogFunc = logFunc
              , appConfRegistry = registry
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
          runRIO (app{dsAppOutDir = oOutDir, dsAppRTSStatsHandle = rtsStatsHandle}) $ do
            logInfo $ "Starting to " <> display oCommand
            writeStreamerHeader
            case oCommand of
              Replay -> replayChain
              Benchmark -> replayBenchmarkReport
              Stats -> replayEpochStats
              ComputeRewards accountIds -> replayRewards accountIds
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
