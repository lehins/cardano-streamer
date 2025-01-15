{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Cardano.Streamer.RTS where

import Cardano.Ledger.BaseTypes (SlotNo (..))
import Cardano.Streamer.Common
import Data.ByteString.Builder (lazyByteString)
import Data.Csv
import Data.Csv.Incremental as CSV
import GHC.Stats

data StreamerStats
  = StreamerStats
  { sSlotNumber :: !SlotNo
  , sRTSStats :: !RTSStats
  }
  deriving (Show)

checkRTSStatsEnabled :: MonadIO m => m ()
checkRTSStatsEnabled = do
  isEnabled <- liftIO getRTSStatsEnabled
  unless (isEnabled) $
    throwString "RTS stats have not been enabled. Use '+RTS -T +RTS -s' in order to do so"

getStreamerStats :: MonadIO m => SlotNo -> m StreamerStats
getStreamerStats slotNo = StreamerStats slotNo <$> liftIO getRTSStats

instance ToRecord StreamerStats where
  toRecord
    StreamerStats
      { sSlotNumber
      , sRTSStats =
        RTSStats
          { gc = GCDetails{..}
          , ..
          }
      } =
      record
        [ toField (unSlotNo sSlotNumber)
        , toField gcs
        , toField major_gcs
        , toField allocated_bytes
        , toField max_live_bytes
        , toField max_large_objects_bytes
        , toField max_compact_bytes
        , toField max_slop_bytes
        , toField max_mem_in_use_bytes
        , toField cumulative_live_bytes
        , toField copied_bytes
        , toField par_copied_bytes
        , toField cumulative_par_max_copied_bytes
        , toField cumulative_par_balanced_copied_bytes
        , toField init_cpu_ns
        , toField init_elapsed_ns
        , toField mutator_cpu_ns
        , toField mutator_elapsed_ns
        , toField gc_cpu_ns
        , toField gc_elapsed_ns
        , toField cpu_ns
        , toField elapsed_ns
        , toField nonmoving_gc_sync_cpu_ns
        , toField nonmoving_gc_sync_elapsed_ns
        , toField nonmoving_gc_sync_max_elapsed_ns
        , toField nonmoving_gc_cpu_ns
        , toField nonmoving_gc_elapsed_ns
        , toField nonmoving_gc_max_elapsed_ns
        , toField gcdetails_gen
        , toField gcdetails_threads
        , toField gcdetails_allocated_bytes
        , toField gcdetails_live_bytes
        , toField gcdetails_large_objects_bytes
        , toField gcdetails_compact_bytes
        , toField gcdetails_slop_bytes
        , toField gcdetails_mem_in_use_bytes
        , toField gcdetails_copied_bytes
        , toField gcdetails_par_max_copied_bytes
        , toField gcdetails_par_balanced_copied_bytes
        , toField gcdetails_block_fragmentation_bytes
        , toField gcdetails_sync_elapsed_ns
        , toField gcdetails_cpu_ns
        , toField gcdetails_elapsed_ns
        , toField gcdetails_nonmoving_gc_sync_cpu_ns
        , toField gcdetails_nonmoving_gc_sync_elapsed_ns
        ]

writeStreamerHeader :: (MonadReader (DbStreamerApp blk) m, MonadIO m) => m ()
writeStreamerHeader = do
  mHandle <- dsAppRTSStatsHandle <$> ask
  forM_ mHandle $ \hdl -> do
    let statsHeader =
          [ "slot_number"
          , "gcs"
          , "major_gcs"
          , "allocated_bytes"
          , "max_live_bytes"
          , "max_large_objects_bytes"
          , "max_compact_bytes"
          , "max_slop_bytes"
          , "max_mem_in_use_bytes"
          , "cumulative_live_bytes"
          , "copied_bytes"
          , "par_copied_bytes"
          , "cumulative_par_max_copied_bytes"
          , "cumulative_par_balanced_copied_bytes"
          , "init_cpu_ns"
          , "init_elapsed_ns"
          , "mutator_cpu_ns"
          , "mutator_elapsed_ns"
          , "gc_cpu_ns"
          , "gc_elapsed_ns"
          , "cpu_ns"
          , "elapsed_ns"
          , "nonmoving_gc_sync_cpu_ns"
          , "nonmoving_gc_sync_elapsed_ns"
          , "nonmoving_gc_sync_max_elapsed_ns"
          , "nonmoving_gc_cpu_ns"
          , "nonmoving_gc_elapsed_ns"
          , "nonmoving_gc_max_elapsed_ns"
          , "gcdetails_gen"
          , "gcdetails_threads"
          , "gcdetails_allocated_bytes"
          , "gcdetails_live_bytes"
          , "gcdetails_large_objects_bytes"
          , "gcdetails_compact_bytes"
          , "gcdetails_slop_bytes"
          , "gcdetails_mem_in_use_bytes"
          , "gcdetails_copied_bytes"
          , "gcdetails_par_max_copied_bytes"
          , "gcdetails_par_balanced_copied_bytes"
          , "gcdetails_block_fragmentation_bytes"
          , "gcdetails_sync_elapsed_ns"
          , "gcdetails_cpu_ns"
          , "gcdetails_elapsed_ns"
          , "gcdetails_nonmoving_gc_sync_cpu_ns"
          , "gcdetails_nonmoving_gc_sync_elapsed_ns"
          ]
    hPutBuilder hdl $ lazyByteString $ CSV.encode $ CSV.encodeRecord $ record statsHeader

writeStreamerStats :: (MonadReader (DbStreamerApp blk) m, MonadIO m) => SlotNo -> m ()
writeStreamerStats slotNo = do
  mHandle <- dsAppRTSStatsHandle <$> ask
  forM_ mHandle (writeStreamerStatsHandle slotNo)

writeStreamerStatsHandle ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m) => SlotNo -> Handle -> m ()
writeStreamerStatsHandle slotNo hdl = do
  stats <- getStreamerStats slotNo
  hPutBuilder hdl $ lazyByteString $ CSV.encode $ CSV.encodeRecord stats
