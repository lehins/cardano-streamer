{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Streamer.Common (
  DbStreamerApp (..),
  AppConfig (..),
  Opts (..),
  Command (..),
  throwExceptT,
  throwShowExceptT,
  throwStringExceptT,
  mkTracer,
  ValidationMode (..),
  HasImmutableDb (..),
  HasResourceRegistry (..),
  parseStakingCredential,
  getElapsedTime,
  getDiskSnapshotFilePath,
  writeReport,
  writeCsv,
  NamedCsv (..),
  writeRecord,
  writeNamedCsv,
  RIO,
  runRIO,
  module X,
) where

import qualified Cardano.Address as A
import qualified Cardano.Address.Style.Shelley as A
import Cardano.Crypto.Hash.Class (hashFromBytes)
import Cardano.Ledger.BaseTypes (EpochNo (..), SlotNo (..))
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Hashes
import Cardano.Ledger.Keys
import Cardano.Streamer.Time
import Control.Monad.Trans.Except
import Control.Tracer (Tracer (..))
import qualified Data.Aeson as Aeson (ToJSON, ToJSONKey, encode)
import Data.ByteString.Builder as BSL (lazyByteString)
import Data.Csv as Csv
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..), snapshotToFileName)
import Ouroboros.Consensus.Util.ResourceRegistry (ResourceRegistry)
import RIO as X hiding (RIO, runRIO)
import RIO.FilePath
import qualified RIO.Text as T
import RIO.Time

type RIO env = ReaderT env IO

runRIO :: MonadIO m => r -> RIO r a -> m a
runRIO env = liftIO . flip runReaderT env

deriving instance Display SlotNo
deriving instance Display EpochNo
deriving instance Aeson.ToJSONKey EpochNo

instance Display DiskSnapshot where
  textDisplay = T.pack . snapshotToFileName

class HasResourceRegistry env where
  registryL :: Lens' env (ResourceRegistry IO)

mkTracer
  :: (MonadReader env m1, MonadIO m2, HasLogFunc env, Show a)
  => Maybe Text
  -- ^ Optional prefix for tracing messages
  -> LogLevel
  -> m1 (Tracer m2 a)
mkTracer mPrefix logLevel = do
  logFunc <- view logFuncL
  return $ Tracer $ \ev ->
    let msg =
          case mPrefix of
            Just prefix -> "[" <> display prefix <> "] " <> displayShow ev
            Nothing -> displayShow ev
     in flip runReaderT logFunc $ logGeneric mempty logLevel msg

throwExceptT :: (Exception e, MonadIO m) => ExceptT e m a -> m a
throwExceptT m =
  runExceptT m >>= \case
    Left exc -> throwIO exc
    Right res -> pure res

throwStringExceptT :: (HasCallStack, MonadIO m) => ExceptT String m a -> m a
throwStringExceptT m =
  runExceptT m >>= \case
    Left exc -> throwString exc
    Right res -> pure res

throwShowExceptT :: (HasCallStack, Show e, MonadIO m) => ExceptT e m a -> m a
throwShowExceptT m = throwStringExceptT $ withExceptT show m

data ValidationMode
  = FullValidation
  | ReValidation
  | NoValidation
  deriving (Eq, Ord, Enum, Bounded, Show)

data DbStreamerApp blk = DbStreamerApp
  { dsAppLogFunc :: !LogFunc
  , dsAppRegistry :: !(ResourceRegistry IO)
  , dsAppProtocolInfo :: !(ProtocolInfo blk)
  , dsAppChainDir :: !FilePath
  , dsAppChainDbArgs :: !(ChainDB.ChainDbArgs Identity IO blk)
  , dsAppIDb :: !(ImmutableDB.ImmutableDB IO blk)
  , dsAppOutDir :: !(Maybe FilePath)
  -- ^ Output directory where to write files
  , dsAppStopSlotNo :: !(Maybe SlotNo)
  -- ^ Last slot number to execute
  , dbAppWriteDiskSnapshots :: ![DiskSnapshot]
  , dsAppValidationMode :: !ValidationMode
  , dsAppStartTime :: !UTCTime
  }

class HasImmutableDb env blk | env -> blk where
  iDbL :: Lens' env (ImmutableDB.ImmutableDB IO blk)

instance HasImmutableDb (DbStreamerApp blk) blk where
  iDbL = lens dsAppIDb $ \app iDb -> app{dsAppIDb = iDb}

instance HasLogFunc (DbStreamerApp blk) where
  logFuncL = lens dsAppLogFunc $ \app logFunc -> app{dsAppLogFunc = logFunc}

instance HasResourceRegistry (DbStreamerApp blk) where
  registryL = lens dsAppRegistry $ \app registry -> app{dsAppRegistry = registry}

data AppConfig = AppConfig
  { appConfChainDir :: !FilePath
  -- ^ Database directory
  , appConfFilePath :: !FilePath
  -- ^ Config file path
  , appConfReadDiskSnapshot :: !(Maybe DiskSnapshot)
  , appConfWriteDiskSnapshots :: ![DiskSnapshot]
  , appConfStopSlotNumber :: !(Maybe Word64)
  , appConfValidationMode :: !ValidationMode
  , appConfLogFunc :: !LogFunc
  , appConfRegistry :: !(ResourceRegistry IO)
  }

instance HasLogFunc AppConfig where
  logFuncL = lens appConfLogFunc $ \app logFunc -> app{appConfLogFunc = logFunc}

instance HasResourceRegistry AppConfig where
  registryL = lens appConfRegistry $ \app registry -> app{appConfRegistry = registry}

data Command
  = Replay
  | Benchmark
  | Stats
  | ComputeRewards (NonEmpty (Credential 'Staking StandardCrypto))
  deriving (Show)

instance Display Command where
  display = \case
    Replay -> "Replay"
    Benchmark -> "Benchmark"
    Stats -> "Statistics"
    ComputeRewards _xs -> "Rewards" -- for: " <> intersperce "," (map displayShow xs)

data Opts = Opts
  { oChainDir :: FilePath
  -- ^ Db directory
  , oConfigFilePath :: FilePath
  -- ^ Config file path
  , oOutDir :: Maybe FilePath
  -- ^ Directory where requested files should be written to.
  , oSnapShotSuffix :: Maybe String
  -- ^ Where to start from
  , oReadSnapShotSlotNumber :: Maybe Word64
  -- ^ Slot number expected for the snapshot to read from.
  , oWriteSnapShotSlotNumbers :: [Word64]
  -- ^ Slot numbers for creating snapshots
  , oStopSlotNumber :: Maybe Word64
  -- ^ Stope replaying the chain once reaching this slot number. When no slot number is
  -- supplied replay will stop only at the end of the immutable chain.
  , oValidationMode :: ValidationMode
  -- ^ What is the level of ledger validation
  , oLogLevel :: LogLevel
  -- ^ Minimum log level
  , oVerbose :: Bool
  -- ^ Verbose logging?
  , oDebug :: Bool
  -- ^ Debug logging?
  , oCommand :: Command
  }
  deriving (Show)

parseStakingCredential :: MonadFail m => Text -> m (Credential 'Staking StandardCrypto)
parseStakingCredential txt =
  case A.fromBech32 txt of
    Nothing -> fail "Can't parse as Bech32 Address"
    Just addr ->
      case A.eitherInspectAddress Nothing addr of
        Left err -> fail $ show err
        Right (A.InspectAddressByron{}) -> fail "Byron Address can't have Staking Crednetial"
        Right (A.InspectAddressIcarus{}) -> fail "Icarus Address can't have Staking Crednetial"
        Right (A.InspectAddressShelley ai) ->
          maybe (fail $ "Address does not contain a Staking Credential: " ++ show txt) pure $
            (KeyHashObj . KeyHash . partialHash <$> A.infoStakeKeyHash ai)
              <|> (ScriptHashObj . ScriptHash . partialHash <$> A.infoStakeScriptHash ai)
  where
    partialHash bs =
      case hashFromBytes bs of
        Nothing -> error $ "Impossible: This should be a valid Hash value: " ++ show bs
        Just h -> h

getElapsedTime :: (MonadReader (DbStreamerApp blk) m, MonadIO m) => m (Time 'Sec)
getElapsedTime = do
  startTime <- dsAppStartTime <$> ask
  curTime <- getCurrentTime
  pure $ diffTimeToSec (diffUTCTime curTime startTime)

getDiskSnapshotFilePath :: MonadReader (DbStreamerApp blk) m => DiskSnapshot -> m FilePath
getDiskSnapshotFilePath diskSnapshot = do
  chainDir <- dsAppChainDir <$> ask
  pure $ chainDir </> "ledger" </> snapshotToFileName diskSnapshot

writeReport
  :: (MonadReader (DbStreamerApp blk) m, MonadIO m, Aeson.ToJSON p, Display p)
  => String
  -> p
  -> m ()
writeReport name report = do
  let reportBuilder = display report
  logInfo reportBuilder
  mOutDir <- dsAppOutDir <$> ask
  forM_ mOutDir $ \outDir -> do
    curTime <- getCurrentTime
    let time = formatTime defaultTimeLocale "%F-%H-%M-%S" curTime
        fileName = name <> "-report-" <> time
        fileNameTxt = fileName <> ".txt"
        fileNameJson = fileName <> ".json"
    writeFileUtf8 (outDir </> fileNameTxt) $ utf8BuilderToText reportBuilder
    writeFileUtf8Builder (outDir </> fileNameJson) $
      Utf8Builder (BSL.lazyByteString $ Aeson.encode report)
    logInfo $
      "Written "
        <> fromString fileNameTxt
        <> " and "
        <> fromString fileNameJson
        <> " to: "
        <> fromString outDir

writeRecord :: (MonadReader (DbStreamerApp blk) m, MonadIO m, ToRecord a) => String -> [a] -> m ()
writeRecord name = writeCsv name . encode

data NamedCsv where
  NamedCsv :: ToNamedRecord a => Csv.Header -> [a] -> NamedCsv

writeNamedCsv
  :: (MonadReader (DbStreamerApp blk) m, MonadIO m)
  => String
  -> NamedCsv
  -> m ()
writeNamedCsv name (NamedCsv csvHeader csv) = writeCsv name (encodeByName csvHeader csv)

writeCsv :: (MonadReader (DbStreamerApp blk) m, MonadIO m) => String -> LByteString -> m ()
writeCsv name csv = do
  mOutDir <- dsAppOutDir <$> ask
  forM_ mOutDir $ \outDir -> do
    curTime <- getCurrentTime
    let time = formatTime defaultTimeLocale "%F-%H-%M-%S" curTime
        fileName = name <> "-data-" <> time <> ".csv"
        filePath = outDir </> fileName
    writeFileBinary filePath $ toStrictBytes csv
    logInfo $ "Written CSV file " <> fromString name <> " report to: " <> fromString filePath
