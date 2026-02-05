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
import Cardano.Crypto.Hash.Class (hashFromBytes, hashFromBytesShort)
import qualified Cardano.Crypto.Hashing as Byron (hashToBytes)
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
import Ouroboros.Consensus.Block (HeaderHash)
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.HardFork.Combinator.AcrossEras (getOneEraHash)
import Ouroboros.Consensus.Protocol.Praos (Praos)
import Ouroboros.Consensus.Protocol.Praos.Header hiding (Header)
import qualified Ouroboros.Consensus.Protocol.Praos.Header as Praos (Header)
import Ouroboros.Consensus.Protocol.TPraos (TPraos)
import Ouroboros.Consensus.Shelley.Ledger.Block hiding (Header)
import Ouroboros.Network.SizeInBytes (SizeInBytes)

-- Import orphan type family instances for ShelleyProtocolHeader:

import Ouroboros.Consensus.Shelley.Protocol.Praos ()
import Ouroboros.Consensus.Shelley.Protocol.TPraos ()

data BlockWithInfo b = BlockWithInfo
  { biSlotNo :: !SlotNo
  , biBlockSize :: !SizeInBytes
  , biBlocksProcessed :: !Word64
  , biBlockHeaderSize :: !Word16
  , biBlockHeaderHash :: !(Hash HASH EraIndependentBlockHeader)
  , biBlockComponent :: !b
  }

class BlockHeaderHash blk where
  toHeaderHash :: proxy blk -> HeaderHash blk -> Hash HASH EraIndependentBlockHeader

instance BlockHeaderHash ByronBlock where
  toHeaderHash _ (ByronHash hash) =
    case hashFromBytes (Byron.hashToBytes hash) of
      Nothing -> error "Impossible: Byron Header Hash is also Blake2b_256"
      Just h -> h

instance BlockHeaderHash (ShelleyBlock proto era) where
  toHeaderHash _ = unShelleyHash

instance BlockHeaderHash (CardanoBlock block) where
  toHeaderHash _ hash =
    case hashFromBytesShort (getOneEraHash hash) of
      Nothing -> error "Impossible: Header Hash for all eras isBlake2b_256"
      Just h -> h

instance Functor BlockWithInfo where
  fmap f bwi = bwi{biBlockComponent = f (biBlockComponent bwi)}

data TxSummary = TxSummary
  { tpSize :: !Word32
  , tpInsCount :: !Word16
  , tpOutsCount :: !Word16
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

blockCardanoEra :: CardanoBlock c -> CardanoEra
blockCardanoEra =
  withProtocolBlock (\_ -> Byron) (\era _ -> era) (\era _ -> era)

withProtocolBlock ::
  (ByronBlock -> a) ->
  (forall era. EraApp era => CardanoEra -> ShelleyBlock (TPraos c) era -> a) ->
  (forall era. EraApp era => CardanoEra -> ShelleyBlock (Praos c) era -> a) ->
  CardanoBlock c ->
  a
withProtocolBlock applyBronBlock applyTPraosBlock applyPraosBlock = \case
  BlockByron byronBlock -> applyBronBlock byronBlock
  BlockShelley shelleyBlock -> applyTPraosBlock Shelley shelleyBlock
  BlockAllegra allegraBlock -> applyTPraosBlock Allegra allegraBlock
  BlockMary maryBlock -> applyTPraosBlock Mary maryBlock
  BlockAlonzo alonzoBlock -> applyTPraosBlock Alonzo alonzoBlock
  BlockBabbage babbageBlock -> applyPraosBlock Babbage babbageBlock
  BlockConway conwayBlock -> applyPraosBlock Conway conwayBlock
  BlockDijkstra conwayBlock -> applyPraosBlock Dijkstra conwayBlock

getBlockSummary :: Crypto c => CardanoBlock c -> BlockSummary
getBlockSummary =
  withProtocolBlock getByronBlockSummary getTPraosBlockSummary getPraosBlockSummary
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
  forall era c.
  (EraBlockBody era, Crypto c) =>
  CardanoEra ->
  ShelleyBlock (TPraos c) era ->
  BlockSummary
getTPraosBlockSummary era block =
  let (blockHeaderBody, blockHeaderSize) =
        case bheader (shelleyBlockRaw block) of
          bh@(BHeader bhBody _) -> (bhBody, originalBytesSize bh)
      (txsSeq, blockSize) = case shelleyBlockRaw block of
        Block _ blockBody -> (blockBody ^. txSeqBlockBodyL, bBodySize (ProtVer (eraProtVerLow @era) 0) blockBody)
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
  forall era c.
  (EraBlockBody era, Crypto c) =>
  CardanoEra ->
  ShelleyBlock (Praos c) era ->
  BlockSummary
getPraosBlockSummary era block =
  let blockHeader = bheader (shelleyBlockRaw block)
      blockHeaderBody = headerBody blockHeader
      blockHeaderSize = headerSize blockHeader
      (txsSeq, blockSize) = case shelleyBlockRaw block of
        Block _ blockBody -> (blockBody ^. txSeqBlockBodyL, bBodySize (ProtVer (eraProtVerLow @era) 0) blockBody)
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
    { tpSize = tx ^. sizeTxF
    , tpInsCount = fromIntegral $ length $ tx ^. bodyTxL . inputsTxBodyL
    , tpOutsCount = fromIntegral $ length $ tx ^. bodyTxL . outputsTxBodyL
    }

withBlockTxs ::
  forall a c.
  ([B.ATxAux ByteString] -> a) ->
  (forall era. EraApp era => [Tx era] -> a) ->
  CardanoBlock c ->
  a
withBlockTxs applyByronTxs applyNonByronTxs =
  withProtocolBlock
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
getShelleyOnwardsTxs = toList . view txSeqBlockBodyL . bbody . shelleyBlockRaw

getSlotNo :: Crypto c => CardanoBlock c -> SlotNo
getSlotNo =
  withProtocolBlock
    byronBlockSlotNo
    (\_ -> bheaderSlotNo . getTPraosBHeaderBody)
    (\_ -> hbSlotNo . getPraosBHeaderBody)

getSlotNoWithEra :: Crypto c => CardanoBlock c -> (CardanoEra, SlotNo)
getSlotNoWithEra =
  withProtocolBlock
    ((,) Byron . byronBlockSlotNo)
    (\era -> (,) era . bheaderSlotNo . getTPraosBHeaderBody)
    (\era -> (,) era . hbSlotNo . getPraosBHeaderBody)

getTPraosBHeaderBody :: Crypto c => ShelleyBlock (TPraos c) era -> BHBody c
getTPraosBHeaderBody block =
  case bheader (shelleyBlockRaw block) of
    BHeader bhBody _ -> bhBody

getPraosBHeaderBody :: Crypto c => ShelleyBlock (Praos c) era -> HeaderBody c
getPraosBHeaderBody block = headerBody (bheader (shelleyBlockRaw block))

type family BlockHeader era where
  BlockHeader ShelleyEra = BHeader StandardCrypto
  BlockHeader AllegraEra = BHeader StandardCrypto
  BlockHeader MaryEra = BHeader StandardCrypto
  BlockHeader AlonzoEra = BHeader StandardCrypto
  BlockHeader BabbageEra = Praos.Header StandardCrypto
  BlockHeader ConwayEra = Praos.Header StandardCrypto
  BlockHeader DijkstraEra = Header StandardCrypto

writeBlock ::
  forall era h m.
  ( Era era
  , EncCBOR (Block h era)
  , MonadIO m
  ) =>
  FilePath -> Block h era -> m ()
writeBlock fp block =
  liftIO $ BS.writeFile fp $ serialize' (eraProtVerLow @era) block

readBlock ::
  forall era m.
  ( Era era
  , DecCBOR (Annotator (Block (BlockHeader era) era))
  , MonadIO m
  ) =>
  FilePath -> m (Block (BlockHeader era) era)
readBlock fp =
  liftIO (BSL.readFile fp) <&> decodeFullAnnotator (eraProtVerLow @era) "Block" decCBOR >>= \case
    Left exc -> throwIO exc
    Right res -> pure res

writeTx :: forall era m. (EraTx era, MonadIO m) => FilePath -> Tx era -> m ()
writeTx fp = liftIO . BSL.writeFile fp . serialize (eraProtVerLow @era)

filterBlockWithdrawals ::
  Set (Credential 'Staking) ->
  CardanoBlock c ->
  Map (Credential 'Staking) Coin
filterBlockWithdrawals creds =
  withBlockTxs (const mempty) $ \txs ->
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
  withBlockTxs (const Map.empty) (calcStatsForAppScripts fTx)

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
