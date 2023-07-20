{-# LANGUAGE RecordWildCards #-}

module Main where

import Cardano.Streamer.Producer
--import Ouroboros.Consensus.Storage.LedgerDB.Snapshots
import CLI

main :: IO ()
main = do
  Args Opts{..} <- parseArgs
  runApp oChainDir oConfigFilePath oOutDir oDiskSnapShot oVerbose -- oLogLevel
    -- "/home/lehins/.local/share/Daedalus/mainnet/chain"
    -- "/home/lehins/iohk/cardano-node/configuration/cardano/mainnet-config.json"
    -- (Just ".")
    -- --(Just (DiskSnapshot 72316896 (Just "babbage_7")))
    -- Nothing --(Just (DiskSnapshot 91242495 Nothing))
    -- False
