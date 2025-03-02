{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Cardano.Streamer.Common (
  App,
  DbStreamerApp (..),
  AppConfig (..),
  Opts (..),
  Command (..),
  CardanoEra (..),
  BlockHashOrSlotNo (..),
  throwExceptT,
  throwShowExceptT,
  throwStringExceptT,
  mkTracer,
  LedgerDb (..),
  HasLedgerDb (..),
  ValidationMode (..),
  HasImmutableDb (..),
  HasResourceRegistry (..),
  formatRewardAccount,
  parseRewardAccount,
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

import qualified Cardano.Address as A (NetworkTag (..), bech32, fromBech32)
import qualified Cardano.Address.KeyHash as A
import qualified Cardano.Address.Script as A
import qualified Cardano.Address.Style.Shelley as A
import Cardano.Crypto.Hash.Class (hashFromBytes, hashToBytes, hashToTextAsHex)
import Cardano.Ledger.Address
import Cardano.Ledger.BaseTypes (
  BlockNo (..),
  EpochNo (..),
  SlotNo (..),
  networkToWord8,
  word8ToNetwork,
 )
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Protocol.Crypto (StandardCrypto)
import Cardano.Streamer.Time
import Control.Monad.Trans.Except
import Control.ResourceRegistry (ResourceRegistry)
import Control.Tracer (Tracer (..))
import qualified Data.Aeson as Aeson (ToJSON, ToJSONKey, encode)
import Data.ByteString.Builder as BSL (lazyByteString)
import qualified Data.ByteString.Char8 as BS8 (pack)
import Data.Csv as Csv
import Data.Fixed
import Data.Time.Format.ISO8601
import Ouroboros.Consensus.Block (Point)
import Ouroboros.Consensus.Cardano.Block (CardanoBlock)
import Ouroboros.Consensus.Node.ProtocolInfo (ProtocolInfo (..))
import qualified Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import qualified Ouroboros.Consensus.Storage.LedgerDB as LDB
import Ouroboros.Consensus.Storage.LedgerDB.Snapshots (DiskSnapshot (..), snapshotToDirName)
import RIO as X hiding (RIO, runRIO)
import RIO.FilePath
import qualified RIO.Text as T
import RIO.Time

-- There are a whole bunch of orphans in Consensus and this is the simplest way to avoid dealing with them:
import Cardano.Api ()

type RIO env = ReaderT env IO

runRIO :: MonadIO m => r -> RIO r a -> m a
runRIO env = liftIO . flip runReaderT env

deriving instance Display SlotNo
deriving instance ToField SlotNo

deriving instance Display EpochNo
deriving instance ToField EpochNo
deriving instance Aeson.ToJSONKey EpochNo

deriving instance Display BlockNo

instance ToField UTCTime where
  toField = BS8.pack . iso8601Show

instance ToField Coin where
  toField (Coin c) =
    BS8.pack $ show (MkFixed c :: Fixed E6)

instance Display DiskSnapshot where
  textDisplay = T.pack . snapshotToDirName

instance Display (Hash v a) where
  textDisplay = hashToTextAsHex

instance Display (SafeHash a) where
  textDisplay = hashToTextAsHex . extractHash

class HasResourceRegistry env where
  registryL :: Lens' env (ResourceRegistry IO)

data CardanoEra
  = Byron
  | Shelley
  | Allegra
  | Mary
  | Alonzo
  | Babbage
  | Conway
  | Dijkstra
  deriving (Eq, Ord, Show, Enum, Bounded)

instance ToField CardanoEra where
  toField = BS8.pack . show

instance Display CardanoEra where
  display = displayShow

mkTracer ::
  (MonadReader env m1, MonadIO m2, HasLogFunc env, Show a) =>
  -- | Optional prefix for tracing messages
  Maybe Text ->
  LogLevel ->
  m1 (Tracer m2 a)
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

type App = DbStreamerApp (CardanoBlock StandardCrypto)

data DbStreamerApp blk = DbStreamerApp
  { dsAppLogFunc :: !LogFunc
  , dsAppRegistry :: !(ResourceRegistry IO)
  , dsAppProtocolInfo :: !(ProtocolInfo blk)
  , dsAppChainDir :: !FilePath
  , dsAppChainDbArgs :: !(ChainDB.ChainDbArgs Identity IO blk)
  , dsAppIDb :: !(ImmutableDB.ImmutableDB IO blk)
  , dsAppLedgerDb :: !(LedgerDb blk)
  , dsAppOutDir :: !(Maybe FilePath)
  -- ^ Output directory where to write files
  , dsAppStartPoint :: !(Point blk)
  -- ^ Slot number from which to start reading the chain from.
  , dsAppStopSlotNo :: !(Maybe SlotNo)
  -- ^ Last slot number to execute
  , dsAppWriteDiskSnapshots :: !(IORef [DiskSnapshot])
  , dsAppWriteBlocks :: !(IORef (Set SlotNo, Set (Hash HASH EraIndependentBlockHeader)))
  , dsAppValidationMode :: !ValidationMode
  , dsAppStartTime :: !UTCTime
  , dsAppRTSStatsHandle :: !(Maybe Handle)
  }

data LedgerDb blk = LedgerDb
  { lLedgerDb :: !(LDB.LedgerDB' IO blk)
  , lTestInternals :: !(LDB.TestInternals' IO blk)
  }

class HasLedgerDb env blk | env -> blk where
  ledgerDbL :: Lens' env (LedgerDb blk)

instance HasLedgerDb (DbStreamerApp blk) blk where
  ledgerDbL = lens dsAppLedgerDb $ \app ledgerDb -> app{dsAppLedgerDb = ledgerDb}

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
  , appConfWriteBlocksSlotNoSet :: !(Set SlotNo)
  , appConfWriteBlocksBlockHashSet :: !(Set (Hash HASH EraIndependentBlockHeader))
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
  | ComputeRewards (NonEmpty RewardAccount)
  deriving (Show)

instance Display Command where
  display = \case
    Replay -> "Replay"
    Benchmark -> "Benchmark"
    Stats -> "Compute Statistics"
    ComputeRewards _xs -> "Compute Rewards" -- for: " <> intersperce "," (map displayShow xs)

newtype BlockHashOrSlotNo = BlockHashOrSlotNo
  {unBlockHashOrSlotNo :: Either SlotNo (Hash HASH EraIndependentBlockHeader)}
  deriving (Eq, Show)

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
  , oWriteBlocks :: [BlockHashOrSlotNo]
  -- ^ Slot numbers for blocks that we need to write to file. If a slot has no block this will cause a failure.
  -- TODO: Improve:
  -- , oWriteBlocksTxs :: Bool
  -- , oWriteBlocksLedgerState :: Bool
  , oStopSlotNumber :: Maybe Word64
  -- ^ Stop replaying the chain once reaching this slot number. When no slot number is
  -- supplied replay will stop only at the end of the immutable chain.
  , oValidationMode :: ValidationMode
  -- ^ What is the level of ledger validation
  , oRTSStatsFilePath :: Maybe FilePath
  -- ^ Path to the file where RTS and GC stats for the execution should be written to.
  , oLogLevel :: LogLevel
  -- ^ Minimum log level
  , oVerbose :: Bool
  -- ^ Verbose logging?
  , oDebug :: Bool
  -- ^ Debug logging?
  , oCommand :: Command
  }
  deriving (Show)

formatRewardAccount :: RewardAccount -> Text
formatRewardAccount RewardAccount{raNetwork, raCredential} =
  either (error . show) A.bech32 $ A.stakeAddress discriminant $ credentialToDelegation raCredential
  where
    discriminant = A.NetworkTag $ fromIntegral @Word8 @Word32 $ networkToWord8 $ raNetwork
    credentialToDelegation = \case
      KeyHashObj (KeyHash kh) ->
        A.DelegationFromKeyHash $ A.KeyHash A.Delegation $ hashToBytes kh
      ScriptHashObj (ScriptHash sh) ->
        A.DelegationFromScriptHash $ A.ScriptHash $ hashToBytes sh

parseRewardAccount :: MonadFail m => Text -> m RewardAccount
parseRewardAccount txt =
  case A.fromBech32 txt of
    Nothing -> fail "Can't parse as Bech32 Address"
    Just addr ->
      case A.eitherInspectAddress Nothing addr of
        Left err -> fail $ show err
        Right (A.InspectAddressByron{}) -> fail "Byron Address can't have Staking Crednetial"
        Right (A.InspectAddressIcarus{}) -> fail "Icarus Address can't have Staking Crednetial"
        Right (A.InspectAddressShelley ai) -> do
          let networkTag = A.unNetworkTag $ A.infoNetworkTag ai
          tag <- maybe (fail $ "Invalid NetworkTag value: " <> show networkTag) pure $ do
            guard (networkTag <= fromIntegral (maxBound :: Word8))
            word8ToNetwork $ fromIntegral @Word32 @Word8 networkTag
          maybe
            (fail $ "Address does not contain a Staking Credential: " ++ show txt)
            (pure . RewardAccount tag)
            $ (KeyHashObj . KeyHash . partialHash <$> A.infoStakeKeyHash ai)
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
  pure $ chainDir </> "ledger" </> snapshotToDirName diskSnapshot

writeReport ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m, Aeson.ToJSON p, Display p) =>
  String ->
  p ->
  m ()
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

writeNamedCsv ::
  (MonadReader (DbStreamerApp blk) m, MonadIO m) =>
  String ->
  NamedCsv ->
  m ()
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
