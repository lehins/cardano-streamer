module Main where

import Cardano.Streamer.Producer

main :: IO ()
main =
  runApp
    "/home/lehins/iohk/chain"
    "/home/lehins/iohk/chain/config/mainnet-config.json"
    Nothing
    True
