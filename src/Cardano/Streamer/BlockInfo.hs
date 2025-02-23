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
import Cardano.Ledger.Val
import Cardano.Protocol.Crypto
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
import Ouroboros.Network.SizeInBytes (SizeInBytes)

-- Import orphan type family instances for ShelleyProtocolHeader:

import Ouroboros.Consensus.Shelley.Protocol.Praos ()
import Ouroboros.Consensus.Shelley.Protocol.TPraos ()

data BlockWithInfo b = BlockWithInfo
  { biSlotNo :: !SlotNo
  , biBlockSize :: !SizeInBytes
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

data RawBlock = RawBlock
  { rawBlockBytes :: !ByteString
  , rawBlockHash :: SafeHash EraIndependentBlockBody
  }

instance SafeToHash RawBlock where
  originalBytes = rawBlockBytes

instance HashAnnotated RawBlock EraIndependentBlockBody

getCardanoEra :: CardanoBlock c -> CardanoEra
getCardanoEra =
  applyBlock (\_ -> Byron) (\era _ -> era) (\era _ -> era)

applyBlock ::
  (ByronBlock -> a) ->
  (forall era. EraApp era => CardanoEra -> ShelleyBlock (TPraos c) era -> a) ->
  (forall era. EraApp era => CardanoEra -> ShelleyBlock (Praos c) era -> a) ->
  CardanoBlock c ->
  a
applyBlock applyBronBlock applyTPraosBlock applyPraosBlock = \case
  BlockByron byronBlock -> applyBronBlock byronBlock
  BlockShelley shelleyBlock -> applyTPraosBlock Shelley shelleyBlock
  BlockAllegra allegraBlock -> applyTPraosBlock Allegra allegraBlock
  BlockMary maryBlock -> applyTPraosBlock Mary maryBlock
  BlockAlonzo alonzoBlock -> applyTPraosBlock Alonzo alonzoBlock
  BlockBabbage babbageBlock -> applyPraosBlock Babbage babbageBlock
  BlockConway conwayBlock -> applyPraosBlock Conway conwayBlock

getRawBlock :: CardanoBlock c -> RawBlock
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

getTPraosBlockSummary ::
  (EraSegWits era, Crypto c) =>
  CardanoEra ->
  ShelleyBlock (TPraos c) era ->
  BlockSummary
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

getPraosBlockSummary ::
  (EraSegWits era, Crypto c) =>
  CardanoEra ->
  ShelleyBlock (Praos c) era ->
  BlockSummary
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

applyBlockTxs ::
  forall a c.
  ([B.ATxAux ByteString] -> a) ->
  (forall era. EraApp era => [Tx era] -> a) ->
  CardanoBlock c ->
  a
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

getShelleyOnwardsTxs ::
  EraApp era =>
  ShelleyBlock (p c) era ->
  [Tx era]
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

filterBlockWithdrawals ::
  Set (Credential 'Staking) ->
  CardanoBlock c ->
  Map (Credential 'Staking) Coin
filterBlockWithdrawals creds =
  applyBlockTxs (const mempty) $ \txs ->
    Map.unionsWith (<+>) $
      map
        ( \tx ->
            let wdrls = Map.mapKeys raCredential $ unWithdrawals (tx ^. bodyTxL . withdrawalsTxBodyL)
             in wdrls `Map.restrictKeys` creds
        )
        txs

accScriptsStats ::
  forall ms c.
  ToMaxScript ms =>
  (forall era. EraApp era => Tx era -> [AppScript]) ->
  CardanoBlock c ->
  Map AppLanguage (ScriptsStats ms)
accScriptsStats fTx =
  applyBlockTxs (const Map.empty) (calcStatsForAppScripts fTx)

calcStatsForAppScripts ::
  (ToMaxScript ms, Foldable f, Foldable t) =>
  (a -> f AppScript) ->
  t a ->
  Map AppLanguage (ScriptsStats ms)
calcStatsForAppScripts f = foldl' accStats Map.empty
  where
    accStats acc =
      Map.unionWith (<>) acc . Map.map (foldMap' toScriptsStats) . scriptsPerLanguage . f

languageStatsTxWits ::
  ToMaxScript ms =>
  CardanoBlock c ->
  Map AppLanguage (ScriptsStats ms)
languageStatsTxWits = accScriptsStats $ \tx -> Map.elems $ appScriptTxWits (tx ^. witsTxL)

languageStatsOutsTxBody ::
  ToMaxScript ms =>
  CardanoBlock c ->
  Map AppLanguage (ScriptsStats ms)
languageStatsOutsTxBody = accScriptsStats $ \tx -> outScriptTxBody (tx ^. bodyTxL)

toScriptsStats :: ToMaxScript ms => AppScript -> ScriptsStats ms
toScriptsStats script =
  let sz = appScriptSize script
   in ScriptsStats
        { lsTotalCount = 1
        , lsTotalSize = sz
        , lsMaxScripts = mkMaxScript script
        , lsMinSize = sz
        }

data ScriptsStats ms = ScriptsStats
  { lsTotalCount :: !Int
  , lsTotalSize :: !Int
  , lsMaxScripts :: !ms
  , lsMinSize :: !Int
  }
  deriving (Generic)

instance ToJSON ms => ToJSON (ScriptsStats ms) where
  toJSON = genericToJSON (defaultOptions{fieldLabelModifier = drop 2})

class Monoid ms => ToMaxScript ms where
  mkMaxScript :: AppScript -> ms

newtype MaxScript = MaxScript SBS.ShortByteString
  deriving (Eq, Ord, Show, Monoid)

instance ToMaxScript MaxScript where
  mkMaxScript = MaxScript . appScriptBytes

instance ToJSON MaxScript where
  toJSON (MaxScript s) = toJSON (MaxScripts $ Map.singleton (SBS.length s) s)

instance Semigroup MaxScript where
  (<>) x1@(MaxScript s1) x2@(MaxScript s2)
    | SBS.length s1 < SBS.length s2 = x2
    | otherwise = x1

newtype MaxScripts = MaxScripts (Map Int SBS.ShortByteString)
  deriving (Eq, Ord, Show, Monoid)

instance ToMaxScript MaxScripts where
  mkMaxScript script =
    let sbs = appScriptBytes script
     in MaxScripts $ Map.singleton (SBS.length sbs) sbs

instance ToJSON MaxScripts where
  toJSON (MaxScripts m) = toJSON $ Map.map (T.decodeLatin1 . BS16.encode . SBS.fromShort) m

dropExcess :: Map Int ShortByteString -> MaxScripts
dropExcess m
  | n <= maxSizeCount = MaxScripts m
  | m' : _ <- drop (n - maxSizeCount) $ iterate Map.deleteMin m = MaxScripts m'
  | otherwise = error "Impossible: head on infinite loop"
  where
    n = Map.size m
    maxSizeCount = 100

instance Semigroup MaxScripts where
  (<>) (MaxScripts m1) (MaxScripts m2) = dropExcess $ Map.union m1 m2

instance Display MaxScript where
  display (MaxScript s) = "MaxScript: " <> display (SBS.length s)

instance Display MaxScripts where
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

instance Semigroup ms => Semigroup (ScriptsStats ms) where
  ls1 <> ls2 =
    ScriptsStats
      { lsTotalCount = lsTotalCount ls1 + lsTotalCount ls2
      , lsTotalSize = lsTotalSize ls1 + lsTotalSize ls2
      , lsMaxScripts = lsMaxScripts ls1 <> lsMaxScripts ls2
      , lsMinSize = min (lsMinSize ls1) (lsMinSize ls2)
      }

instance Monoid ms => Monoid (ScriptsStats ms) where
  mempty =
    ScriptsStats
      { lsTotalCount = 0
      , lsTotalSize = 0
      , lsMaxScripts = mempty
      , lsMinSize = maxBound
      }

instance Display ms => Display (ScriptsStats ms) where
  display ScriptsStats{..} =
    "Count: "
      <> display lsTotalCount
      <> " Size: "
      <> display lsTotalSize
      <> " MaxScripts: "
      <> display lsMaxScripts
      <> " MinSize: "
      <> display lsMinSize
