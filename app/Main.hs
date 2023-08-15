module Main where

import Cardano.Streamer.Producer
import CLI

main :: IO ()
main = do
  Args opts <- parseArgs
  runApp opts
    -- "/home/lehins/.local/share/Daedalus/mainnet/chain"
    -- "/home/lehins/iohk/cardano-node/configuration/cardano/mainnet-config.json"
    -- (Just ".")
    -- --(Just (DiskSnapshot 72316896 (Just "babbage_7")))
    -- Nothing --(Just (DiskSnapshot 91242495 Nothing))
    -- False
