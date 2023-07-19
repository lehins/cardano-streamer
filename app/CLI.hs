{-# LANGUAGE LambdaCase #-}

module CLI where

import Control.Applicative
import Options.Applicative
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots
import RIO
import RIO.Text as T

data Opts = Opts
  { oChainDir :: FilePath
  , oConfigFilePath :: FilePath
  , oOutDir :: Maybe FilePath
  , oDiskSnapShot :: Maybe DiskSnapshot
  , oLogLevel :: LogLevel
  , oVerbose :: Bool
  }
  deriving (Show)

readLogLevel :: ReadM LogLevel
readLogLevel =
  maybeReader $ \case
    "debug" -> Just LevelDebug
    "info" -> Just LevelInfo
    "warn" -> Just LevelWarn
    "error" -> Just LevelError
    _ -> Nothing

readDiskSnapshot :: Parser DiskSnapshot
readDiskSnapshot =
  DiskSnapshot
    <$> option
      auto
      ( long "snapshot-slot"
          <> short 's'
          <> help
            ( mconcat
                [ "Read a Snapshot with this slot number. "
                , "Results in failure a snapshot does not exist."
                ]
            )
      )
    <*> option
      (Just <$> str)
      ( long "snapshot-suffix"
          <> value Nothing
          <> help
            ( mconcat
                [ "Read a Snapshot with this suffix. By default no suffix is expected"
                , "Eg. `--chain-db /path/chain -s 123456 --snapshot-suffix mysufix' will read a "
                , " snapshot with this file path /path/chain/ledger/1234656_mysuffix"
                ]
            )
      )

optsParser :: Parser Opts
optsParser =
  Opts
    <$> strOption
      ( long "chain-dir"
          <> metavar "CHAIN_DIR"
          <> help
            ( mconcat
                [ "Path to the directory where Cardano Chain data is located. "
                , "Eg. --db-dir=\"~/.local/share/Daedalus/mainnet/chain\""
                ]
            )
      )
    <*> strOption
      ( long "config"
          <> metavar "CONFIG"
          <> help
            ( mconcat
                [ "Cardano configuration file, eg. "
                , "/path/to/cardano-node/configuration/cardano/mainnet-config.json"
                ]
            )
      )
    <*> option
      (Just <$> str)
      ( long "out-dir"
          <> metavar "OUT_DIR"
          <> value Nothing
          <> help "Path to the directory where output files will be written to."
      )
    <*> (fmap Just readDiskSnapshot <|> pure Nothing)
    <*> option
      (readLogLevel)
      ( long "log-level"
          <> metavar "LOG_LEVEL"
          <> value LevelWarn
          <> help "Set the minimum log level: 'debug|info|warn|error'. Default is 'warn'"
      )
    <*> switch (long "verbose" <> short 'v' <> help "Enable verbose output")

data Args = Args
  { argsOpts :: Opts
  }
  deriving (Show)

parseArgs :: IO Args
parseArgs =
  execParser $
    info
      ( Args
          <$> optsParser
          <* abortOption
            (ShowHelpText Nothing)
            (long "help" <> short 'h' <> help "Display this message.")
      )
      ( header "cstreamer - Iterator over the Cardano blockchain"
          <> progDesc
            ( "This tool is designed for analyzing data that lives on Cardano chain as well"
                <> " as analyzing the functionality of the Cardano implementation itself."
            )
          <> fullDesc
      )
