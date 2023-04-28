module Main where

import Cardano.Streamer.Producer
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots

main :: IO ()
main =
  runApp
    "/home/lehins/iohk/chain"
    "/home/lehins/iohk/chain/config/mainnet-config.json"
    (Just "/home/lehins/tmp/")
    -- (Just (DiskSnapshot 72316896 (Just "babbage_7")))
    (Just (DiskSnapshot 87870047 (Just "db-analyser")))
    True
