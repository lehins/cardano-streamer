{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Cardano.Streamer.BlockInfo where

import qualified Cardano.Chain.Block as B
import qualified Cardano.Chain.UTxO as B
import qualified Cardano.Chain.Update as B
import Cardano.Ledger.Address
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Binary
import Cardano.Ledger.Block
import Cardano.Ledger.Coin
import Cardano.Ledger.Core
import Cardano.Ledger.Credential
import Cardano.Ledger.Crypto
import Cardano.Ledger.Keys
import Cardano.Ledger.SafeHash
import Cardano.Ledger.Val
import Cardano.Protocol.TPraos.BHeader
import Cardano.Streamer.Common
import Cardano.Streamer.Ledger
import Control.Monad.Trans.Fail.String (errorFail)
import Data.Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as BS16
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Short as SBS
import Data.Foldable (foldMap')
import Data.List (intersperse)
import qualified Data.Map.Strict as Map
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Protocol.Praos (Praos)
import Ouroboros.Consensus.Protocol.Praos.Header hiding (Header)
import Ouroboros.Consensus.Protocol.TPraos (TPraos)
import Ouroboros.Consensus.Shelley.Ledger.Block hiding (Header)

-- Import orphan type family instances for ShelleyProtocolHeader:

import Ouroboros.Consensus.Shelley.Protocol.Praos ()
import Ouroboros.Consensus.Shelley.Protocol.TPraos ()

data BlockWithInfo b = BlockWithInfo
  { biSlotNo :: !SlotNo
  , biBlockSize :: !Word32
  , biBlockHeaderSize :: !Word16
  , biBlocksProcessed :: !Word64
  , biBlockComponent :: !b
  }

instance Functor BlockWithInfo where
  fmap f bwi = bwi{biBlockComponent = f (biBlockComponent bwi)}

data CardanoEra
  = Byron
  | Shelley
  | Allegra
  | Mary
  | Alonzo
  | Babbage
  | Conway
  deriving (Eq, Show)

instance Display CardanoEra where
  display = displayShow

data TxSummary = TxSummary
  { tpSize :: !Int32
  , tpInsCount :: !Int16
  , tpOutsCount :: !Int16
  }

data BlockSummary = BlockSummary
  { bpEra :: !CardanoEra
  , --
    -- In Byron a boundary block changes this value, otherwise it will be inferred from the
    -- ledger state
    bpSlotNo :: !SlotNo
  , bpBlockNo :: !BlockNo
  , -- , bpAbsBlockNo :: !Word64
    bpProtVer :: Maybe ProtVer
  , bpBlockSize :: Int
  , bpBlockBodySize :: Int
  , bpBlockHeaderSize :: Int
  , bpTxsSummary :: V.Vector TxSummary
  }

fromByronProtocolVersion :: B.ProtocolVersion -> ProtVer
fromByronProtocolVersion pv =
  ProtVer
    { pvMajor = errorFail $ mkVersion $ B.pvMajor pv
    , pvMinor = fromIntegral $ B.pvMinor pv
    }

data RawBlock c = RawBlock
  { rawBlockBytes :: !ByteString
  , rawBlockHash :: SafeHash c EraIndependentBlockBody
  }

instance SafeToHash (RawBlock c) where
  originalBytes = rawBlockBytes

instance HashAnnotated (RawBlock c) EraIndependentBlockBody c

getCardanoEra :: Crypto c => CardanoBlock c -> CardanoEra
getCardanoEra =
  applyBlock (\_ -> Byron) (\era _ -> era) (\era _ -> era)

applyBlock
  :: Crypto c
  => (ByronBlock -> a)
  -> (forall era. EraApp era c => CardanoEra -> ShelleyBlock (TPraos (EraCrypto era)) era -> a)
  -> (forall era. EraApp era c => CardanoEra -> ShelleyBlock (Praos (EraCrypto era)) era -> a)
  -> CardanoBlock c
  -> a
applyBlock applyBronBlock applyTPraosBlock applyPraosBlock = \case
  BlockByron byronBlock -> applyBronBlock byronBlock
  BlockShelley shelleyBlock -> applyTPraosBlock Shelley shelleyBlock
  BlockAllegra allegraBlock -> applyTPraosBlock Allegra allegraBlock
  BlockMary maryBlock -> applyTPraosBlock Mary maryBlock
  BlockAlonzo alonzoBlock -> applyTPraosBlock Alonzo alonzoBlock
  BlockBabbage babbageBlock -> applyPraosBlock Babbage babbageBlock
  BlockConway conwayBlock -> applyPraosBlock Conway conwayBlock

getRawBlock :: Crypto c => CardanoBlock c -> RawBlock c
getRawBlock =
  mkRawBlock . applyBlock byronBlockBytes blockBytes blockBytes
  where
    byronBlockBytes byronBlock =
      case byronBlockRaw byronBlock of
        B.ABOBBlock abBlock -> B.blockAnnotation abBlock
        B.ABOBBoundary abBlock -> B.boundaryAnnotation abBlock
    blockBytes _ block =
      case shelleyBlockRaw block of
        Block' _ _ bs -> toStrictBytes bs
    mkRawBlock bs =
      let rb =
            RawBlock
              { rawBlockBytes = bs
              , rawBlockHash = error "Hash has not been computed yet"
              }
          safeHash = hashAnnotated rb
       in rb{rawBlockHash = safeHash}

getBlockSummary :: Crypto c => CardanoBlock c -> BlockSummary
getBlockSummary =
  applyBlock getByronBlockSummary getTPraosBlockSummary getPraosBlockSummary
  where
    getByronBlockSummary byronBlock =
      case byronBlockRaw byronBlock of
        B.ABOBBlock abBlock ->
          let bbHeader = B.blockHeader abBlock
              bbBody = B.blockBody abBlock
              bbBlockSize = BS.length (B.blockAnnotation abBlock)
              bbHeaderSize = BS.length (B.headerAnnotation bbHeader)
              B.ATxPayload atxs = B.bodyTxPayload bbBody
              byronATxAuxSummary atx =
                let tx = B.taTx atx
                 in TxSummary
                      { tpSize = fromIntegral $ BS.length (B.aTaAnnotation atx)
                      , tpInsCount = fromIntegral $ length (B.txInputs tx)
                      , tpOutsCount = fromIntegral $ length (B.txOutputs tx)
                      }
              bProtVer = fromByronProtocolVersion (B.headerProtocolVersion bbHeader)
           in BlockSummary
                { bpEra = Byron
                , bpSlotNo = byronBlockSlotNo byronBlock
                , bpBlockNo = 0 -- TODO: compute from bpAbsBlockNo and slotNo
                -- , bpAbsBlockNo = absBlockNo
                , bpProtVer = Just bProtVer
                , bpBlockSize = bbBlockSize
                , bpBlockBodySize = bbBlockSize - bbHeaderSize
                , bpBlockHeaderSize = bbHeaderSize
                , bpTxsSummary = V.fromList $! byronATxAuxSummary <$!> atxs
                }
        B.ABOBBoundary abBlock ->
          let bbHeader = B.boundaryHeader abBlock
              bbBody = B.boundaryBody abBlock
              bbHeaderSize = BS.length (B.boundaryHeaderAnnotation bbHeader)
           in BlockSummary
                { bpEra = Byron
                , -- , bpEpochNo = EpochNo (B.boundaryEpoch bbHeader)
                  bpSlotNo = byronBlockSlotNo byronBlock
                , bpBlockNo = 0 -- TODO: compute from absBlockNo and slotNo
                -- , bpAbsBlockNo = absBlockNo
                , bpProtVer = Nothing
                , bpBlockSize = fromIntegral (B.boundaryBlockLength abBlock) --
                , bpBlockBodySize = BS.length (B.boundaryBodyAnnotation bbBody)
                , bpBlockHeaderSize = bbHeaderSize
                , bpTxsSummary = V.empty
                }

getTPraosBlockSummary
  :: (EraSegWits era, Crypto c)
  => CardanoEra
  -> ShelleyBlock (TPraos c) era
  -> BlockSummary
getTPraosBlockSummary era block =
  let (blockHeaderBody, blockHeaderSize) =
        case bheader (shelleyBlockRaw block) of
          bh@(BHeader bhBody _) -> (bhBody, bHeaderSize bh)
      (txsSeq, blockSize) = case shelleyBlockRaw block of
        Block' _ txs bs -> (fromTxSeq txs, fromIntegral (BSL.length bs))
      blockBodySize = fromIntegral (bsize blockHeaderBody)
   in assert (blockSize == blockHeaderSize + blockBodySize) $
        BlockSummary
          { bpEra = era
          , bpSlotNo = bheaderSlotNo blockHeaderBody
          , bpBlockNo = bheaderBlockNo blockHeaderBody
          , -- , bpAbsBlockNo = absBlockNo
            bpProtVer = Just $! bprotver blockHeaderBody
          , bpBlockSize = blockSize
          , bpBlockBodySize = blockBodySize
          , bpBlockHeaderSize = blockHeaderSize
          , bpTxsSummary = getTxsSummary txsSeq
          }

getPraosBlockSummary
  :: (EraSegWits era, Crypto c)
  => CardanoEra
  -> ShelleyBlock (Praos c) era
  -> BlockSummary
getPraosBlockSummary era block =
  let blockHeader = bheader (shelleyBlockRaw block)
      blockHeaderBody = headerBody blockHeader
      blockHeaderSize = headerSize blockHeader
      (txsSeq, blockSize) = case shelleyBlockRaw block of
        Block' _ txs bs -> (fromTxSeq txs, fromIntegral (BSL.length bs))
      blockBodySize = fromIntegral (hbBodySize blockHeaderBody)
   in assert (blockSize == blockHeaderSize + blockBodySize) $
        BlockSummary
          { bpEra = era
          , bpSlotNo = hbSlotNo blockHeaderBody
          , bpBlockNo = hbBlockNo blockHeaderBody
          , -- , bpAbsBlockNo = absBlockNo
            bpProtVer = Just $ hbProtVer blockHeaderBody
          , bpBlockSize = blockSize
          , bpBlockBodySize = blockBodySize
          , bpBlockHeaderSize = blockHeaderSize
          , bpTxsSummary = getTxsSummary txsSeq
          }

getTxsSummary :: (EraTx era, Foldable t) => t (Tx era) -> Vector TxSummary
getTxsSummary txsSeq =
  V.fromList $! getTxSummary <$!> toList txsSeq

getTxSummary :: EraTx era => Tx era -> TxSummary
getTxSummary tx =
  TxSummary
    { tpSize = fromInteger (tx ^. sizeTxF)
    , tpInsCount = fromIntegral $ length $ tx ^. bodyTxL . inputsTxBodyL
    , tpOutsCount = fromIntegral $ length $ tx ^. bodyTxL . outputsTxBodyL
    }

applyBlockTxs
  :: forall a c
   . Crypto c
  => ([B.ATxAux ByteString] -> a)
  -> (forall era. EraApp era c => [Tx era] -> a)
  -> CardanoBlock c
  -> a
applyBlockTxs applyByronTxs applyNonByronTxs =
  applyBlock
    (applyByronTxs . getByronTxs)
    (\_ -> applyNonByronTxs . getShelleyOnwardsTxs)
    (\_ -> applyNonByronTxs . getShelleyOnwardsTxs)

getByronTxs :: ByronBlock -> [B.ATxAux ByteString]
getByronTxs byronBlock =
  case byronBlockRaw byronBlock of
    B.ABOBBlock abBlock ->
      let B.ATxPayload atxs = B.bodyTxPayload (B.blockBody abBlock)
       in atxs
    B.ABOBBoundary _abBlock -> []

getShelleyOnwardsTxs
  :: EraApp era c
  => ShelleyBlock (p (EraCrypto era)) era
  -> [Tx era]
getShelleyOnwardsTxs = toList . fromTxSeq . bbody . shelleyBlockRaw

getSlotNo :: Crypto c => CardanoBlock c -> SlotNo
getSlotNo =
  applyBlock
    byronBlockSlotNo
    (\_ -> bheaderSlotNo . getTPraosBHeaderBody)
    (\_ -> hbSlotNo . getPraosBHeaderBody)

getSlotNoWithEra :: Crypto c => CardanoBlock c -> (CardanoEra, SlotNo)
getSlotNoWithEra =
  applyBlock
    ((,) Byron . byronBlockSlotNo)
    (\era -> (,) era . bheaderSlotNo . getTPraosBHeaderBody)
    (\era -> (,) era . hbSlotNo . getPraosBHeaderBody)

getTPraosBHeaderBody :: Crypto c => ShelleyBlock (TPraos c) era -> BHBody c
getTPraosBHeaderBody block =
  case bheader (shelleyBlockRaw block) of
    BHeader bhBody _ -> bhBody

getPraosBHeaderBody :: Crypto c => ShelleyBlock (Praos c) era -> HeaderBody c
getPraosBHeaderBody block = headerBody (bheader (shelleyBlockRaw block))

writeTx :: forall era m. (EraTx era, MonadIO m) => FilePath -> Tx era -> m ()
writeTx fp = liftIO . BSL.writeFile fp . serialize (eraProtVerLow @era)

filterBlockWithdrawals
  :: Crypto c
  => Set (Credential 'Staking c)
  -> CardanoBlock c
  -> Map (Credential 'Staking c) Coin
filterBlockWithdrawals creds =
  applyBlockTxs (const mempty) $ \txs ->
    Map.unionsWith (<+>) $
      map
        ( \tx ->
            let wdrls = Map.mapKeys getRwdCred $ unWithdrawals (tx ^. bodyTxL . withdrawalsTxBodyL)
             in wdrls `Map.restrictKeys` creds
        )
        txs

accScriptsStats
  :: forall c
   . Crypto c
  => (forall era. EraApp era c => Tx era -> [AppScript])
  -> CardanoBlock c
  -> Map AppLanguage ScriptsStats
accScriptsStats fTx =
  applyBlockTxs (const Map.empty) (calcStatsForAppScripts fTx)

calcStatsForAppScripts
  :: (Foldable f, Foldable t)
  => (a -> f AppScript)
  -> t a
  -> Map AppLanguage ScriptsStats
calcStatsForAppScripts f = foldl' accStats Map.empty
  where
    accStats acc =
      Map.unionWith (<>) acc . Map.map (foldMap' toScriptsStats) . scriptsPerLanguage . f

languageStatsTxWits :: forall c. Crypto c => CardanoBlock c -> Map AppLanguage ScriptsStats
languageStatsTxWits = accScriptsStats $ \tx -> Map.elems $ appScriptTxWits (tx ^. witsTxL)

languageStatsOutsTxBody :: forall c. Crypto c => CardanoBlock c -> Map AppLanguage ScriptsStats
languageStatsOutsTxBody = accScriptsStats $ \tx -> outScriptTxBody (tx ^. bodyTxL)

toScriptsStats :: AppScript -> ScriptsStats
toScriptsStats script =
  let sz = appScriptSize script
   in ScriptsStats
        { lsTotalCount = 1
        , lsTotalSize = sz
        , lsMaxScripts = MaxScript sz (appScriptBytes script)
        , lsMinSize = sz
        }

data ScriptsStats = ScriptsStats
  { lsTotalCount :: !Int
  , lsTotalSize :: !Int
  , lsMaxScripts :: !MaxScripts
  , lsMinSize :: !Int
  }
  deriving (Generic)

instance ToJSON ScriptsStats where
  toJSON = genericToJSON (defaultOptions{fieldLabelModifier = drop 2})

-- | By default this type will retain one maximum script. However, as soon as `MaxScripts`
-- is encountered in an append, up to a 100 will be retained.
data MaxScripts
  = MaxScript !Int SBS.ShortByteString
  | MaxScripts !(Map Int SBS.ShortByteString)
  deriving (Eq, Ord, Show)

instance ToJSON MaxScripts where
  toJSON = \case
    MaxScript n s -> toJSON (MaxScripts $ Map.singleton n s)
    MaxScripts m -> toJSON $ Map.map (T.decodeLatin1 . BS16.encode . SBS.fromShort) m

dropExcess :: Map Int ShortByteString -> MaxScripts
dropExcess m
  | n <= maxSizeCount = MaxScripts m
  | m' : _ <- drop (maxSizeCount - n) $ iterate Map.deleteMin m = MaxScripts m'
  | otherwise = error "Impossible: head on infinite loop"
  where
    n = Map.size m
    maxSizeCount = 100

instance Semigroup MaxScripts where
  (<>) (MaxScripts m1) (MaxScripts m2) = dropExcess $ Map.union m1 m2
  (<>) (MaxScript n1 s1) (MaxScripts m2) = dropExcess $ Map.insert n1 s1 m2
  (<>) (MaxScripts m1) (MaxScript n2 s2) = dropExcess $ Map.alter (maybe (Just s2) Just) n2 m1
  (<>) x1@(MaxScript n1 _) x2@(MaxScript n2 _)
    | n1 < n2 = x2
    | otherwise = x1

instance Monoid MaxScripts where
  mempty = MaxScript 0 ""

instance Display MaxScripts where
  display (MaxScript n _) = "MaxScript: " <> display n
  display (MaxScripts m) = "MaxScripts<" <> display (Map.size m) <> ">" <> content
    where
      n = Map.size m
      keys = Map.keys m
      keysEnd = reverse $ take 3 $ reverse keys
      inter xs = mconcat $ intersperse "," $ map display xs
      content
        | n == 0 = ""
        | n < 7 = ": [" <> inter (map display keys) <> "]"
        | otherwise =
            display $ ": [" <> inter (take 3 keys) <> "..." <> inter keysEnd <> "]"

instance Semigroup ScriptsStats where
  ls1 <> ls2 =
    ScriptsStats
      { lsTotalCount = lsTotalCount ls1 + lsTotalCount ls2
      , lsTotalSize = lsTotalSize ls1 + lsTotalSize ls2
      , lsMaxScripts = lsMaxScripts ls1 <> lsMaxScripts ls2
      , lsMinSize = min (lsMinSize ls1) (lsMinSize ls2)
      }

instance Monoid ScriptsStats where
  mempty =
    ScriptsStats
      { lsTotalCount = 0
      , lsTotalSize = 0
      , lsMaxScripts = mempty
      , lsMinSize = maxBound
      }

instance Display ScriptsStats where
  display ScriptsStats{..} =
    "Count: "
      <> display lsTotalCount
      <> " Size: "
      <> display lsTotalSize
      <> " MaxScripts: "
      <> display lsMaxScripts
      <> " MinSize: "
      <> display lsMinSize
