module Main where

import CLI
import Cardano.Streamer.Run

main :: IO ()
main = do
  Args opts <- parseArgs
  runApp opts
