{-# LANGUAGE LambdaCase #-}

module CLI where

import Cardano.Streamer.Common
import qualified Data.List.NonEmpty as NE
import Options.Applicative
import RIO.Text as T

readLogLevel :: ReadM LogLevel
readLogLevel =
  maybeReader $ \case
    "debug" -> Just LevelDebug
    "info" -> Just LevelInfo
    "warn" -> Just LevelWarn
    "error" -> Just LevelError
    _ -> Nothing

readValidationMode :: ReadM ValidationMode
readValidationMode =
  maybeReader $ \case
    "full" -> Just FullValidation
    "re" -> Just ReValidation
    "none" -> Just NoValidation
    _ -> Nothing

optsParser :: Parser Opts
optsParser =
  Opts
    <$> strOption
      ( long "chain-dir"
          <> metavar "CHAIN_DIR"
          <> help
            ( mconcat
                [ "Path to the directory where Cardano Chain data is located, "
                , "eg. --db-dir=\"~/.local/share/Daedalus/mainnet/chain\""
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
    <*> ( option
            (Just <$> str)
            ( long "suffix"
                <> value Nothing
                <> help
                  ( mconcat
                      [ "Read and a write Snapshots with this suffix, "
                      , "eg. `--chain-db /path/to/chain -r 123456 --suffix my_suffix' will read "
                      , "a snapshot from a file with this file path: "
                      , "/path/to/chain/ledger/1234656_my_suffix "
                      , "By default no suffix is expected."
                      ]
                  )
            )
            <|> pure Nothing
        )
    <*> ( option
            auto
            ( long "read-snapshot"
                <> short 'r'
                <> help
                  ( mconcat
                      [ "Read a Snapshot with this slot number. "
                      , "Also see a relevant --snapshot-suffix flag. "
                      , "Results in failure if a snapshot does not exist."
                      ]
                  )
            )
            <|> pure Nothing
        )
    <*> many
      ( option
          auto
          ( long "write-snapshot"
              <> short 'w'
              <> help
                ( mconcat
                    [ "Write a Snapshot with this slot number. "
                    , "Snapshots for many slot numbers can be written in a single run. "
                    , "Also see a relevant --snapshot-suffix flag. "
                    -- , "Results in failure if a slot does not exist." -- does it?
                    ]
                )
          )
      )
    <*> ( option
            auto
            ( long "stop"
                <> short 's'
                <> help
                  ( mconcat
                      [ "Stop replaying the chain at this slot number. "
                      , "When no stopping slot number is provided, then the replay will "
                      , "continue until the end of the immutable chain is reached."
                      ]
                  )
            )
            <|> pure Nothing
        )
    <*> option
      readValidationMode
      ( long "validate"
          <> metavar "VALIDATE"
          <> value FullValidation
          <> help "Set the validation level: 'full|re|none'. Default is 'full'"
      )
    <*> option
      readLogLevel
      ( long "log-level"
          <> metavar "LOG_LEVEL"
          <> value LevelWarn
          <> help "Set the minimum log level: 'debug|info|warn|error'. Default is 'warn'"
      )
    <*> switch (long "verbose" <> short 'v' <> help "Enable verbose output")
    <*> switch (long "debug" <> short 'd' <> help "Enable debug output")
    <*> commandParser

commandParser :: Parser Command
commandParser =
  subparser
    ( metavar "replay"
        <> command "replay" (info (pure Replay) (progDesc "Replay the chain"))
    )
    <|> subparser
      ( metavar "benchmark"
          <> command
            "benchmark"
            ( info
                (pure Benchmark)
                ( progDesc $
                    mconcat
                      [ "Replay the chanin with timing how long processing each step takes and "
                      , "produce simple statistics at the end."
                      ]
                )
            )
      )
    <|> subparser
      ( metavar "rewards"
          <> command
            "rewards"
            (info rewardsCommandParser (progDesc "Calculate Rewards and Wthdrawals per Epoch"))
      )

rewardsCommandParser :: Parser Command
rewardsCommandParser =
  ( ComputeRewards
      <$> NE.some1
        ( option
            (readWithMaybe parseStakingCredential)
            (long "address" <> help "Bech32 encoded Addres with 'addr' prefix")
        )
  )

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

readWithMaybe :: (Text -> Maybe a) -> ReadM a
readWithMaybe f = maybeReader (f . T.pack)
