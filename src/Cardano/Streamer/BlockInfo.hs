{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
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
import Control.Monad.Trans.Fail.String (errorFail)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V

-- import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Byron.Ledger.Block
import Ouroboros.Consensus.Cardano.Block
import Ouroboros.Consensus.Protocol.Praos (Praos)
import Ouroboros.Consensus.Protocol.Praos.Header hiding (Header)
import Ouroboros.Consensus.Protocol.TPraos (TPraos)
import Ouroboros.Consensus.Shelley.Ledger.Block hiding (Header)

-- Import orphan type family instances for ShelleyProtocolHeader:

import Ouroboros.Consensus.Shelley.Protocol.Praos ()
import Ouroboros.Consensus.Shelley.Protocol.TPraos ()

data BlockEra
  = Byron
  | Shelley
  | Allegra
  | Mary
  | Alonzo
  | Babbage
  | Conway
  deriving (Eq, Show)

instance Display BlockEra where
  display = displayShow

data TxPrecis = TxPrecis
  { tpSize :: !Int32
  , tpInsCount :: !Int16
  , tpOutsCount :: !Int16
  }

data BlockPrecis = BlockPrecis
  { bpEra :: !BlockEra
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
  , bpTxsPrecis :: V.Vector TxPrecis
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

applyBlock
  :: (ByronBlock -> a)
  -> ( forall era
        . (EraSegWits era, EraCrypto era ~ StandardCrypto)
       => BlockEra
       -> ShelleyBlock (TPraos (EraCrypto era)) era
       -> a
     )
  -> ( forall era
        . (EraSegWits era, EraCrypto era ~ StandardCrypto)
       => BlockEra
       -> ShelleyBlock (Praos (EraCrypto era)) era
       -> a
     )
  -> CardanoBlock StandardCrypto
  -> a
applyBlock applyBronBlock applyTPraosBlock applyPraosBlock = \case
  BlockByron byronBlock -> applyBronBlock byronBlock
  BlockShelley shelleyBlock -> applyTPraosBlock Shelley shelleyBlock
  BlockAllegra allegraBlock -> applyTPraosBlock Allegra allegraBlock
  BlockMary maryBlock -> applyTPraosBlock Mary maryBlock
  BlockAlonzo alonzoBlock -> applyTPraosBlock Alonzo alonzoBlock
  BlockBabbage babbageBlock -> applyPraosBlock Babbage babbageBlock
  BlockConway conwayBlock -> applyPraosBlock Conway conwayBlock

getRawBlock :: CardanoBlock StandardCrypto -> RawBlock StandardCrypto
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

getBlockPrecis :: CardanoBlock StandardCrypto -> BlockPrecis
getBlockPrecis =
  applyBlock getByronBlockPrecis getTPraosBlockPrecis getPraosBlockPrecis
  where
    getByronBlockPrecis byronBlock =
      case byronBlockRaw byronBlock of
        B.ABOBBlock abBlock ->
          let bbHeader = B.blockHeader abBlock
              bbBody = B.blockBody abBlock
              bbBlockSize = BS.length (B.blockAnnotation abBlock)
              bbHeaderSize = BS.length (B.headerAnnotation bbHeader)
              B.ATxPayload atxs = B.bodyTxPayload bbBody
              byronATxAuxPrecis atx =
                let tx = B.taTx atx
                 in TxPrecis
                      { tpSize = fromIntegral $ BS.length (B.aTaAnnotation atx)
                      , tpInsCount = fromIntegral $ length (B.txInputs tx)
                      , tpOutsCount = fromIntegral $ length (B.txOutputs tx)
                      }
              bProtVer = fromByronProtocolVersion (B.headerProtocolVersion bbHeader)
           in BlockPrecis
                { bpEra = Byron
                , bpSlotNo = byronBlockSlotNo byronBlock
                , bpBlockNo = 0 -- TODO: compute from bpAbsBlockNo and slotNo
                -- , bpAbsBlockNo = absBlockNo
                , bpProtVer = Just bProtVer
                , bpBlockSize = bbBlockSize
                , bpBlockBodySize = bbBlockSize - bbHeaderSize
                , bpBlockHeaderSize = bbHeaderSize
                , bpTxsPrecis = V.fromList $! byronATxAuxPrecis <$!> atxs
                }
        B.ABOBBoundary abBlock ->
          let bbHeader = B.boundaryHeader abBlock
              bbBody = B.boundaryBody abBlock
              bbHeaderSize = BS.length (B.boundaryHeaderAnnotation bbHeader)
           in BlockPrecis
                { bpEra = Byron
                , -- , bpEpochNo = EpochNo (B.boundaryEpoch bbHeader)
                  bpSlotNo = byronBlockSlotNo byronBlock
                , bpBlockNo = 0 -- TODO: compute from absBlockNo and slotNo
                -- , bpAbsBlockNo = absBlockNo
                , bpProtVer = Nothing
                , bpBlockSize = fromIntegral (B.boundaryBlockLength abBlock) --
                , bpBlockBodySize = BS.length (B.boundaryBodyAnnotation bbBody)
                , bpBlockHeaderSize = bbHeaderSize
                , bpTxsPrecis = V.empty
                }

getTPraosBlockPrecis
  :: (EraSegWits era, Crypto c)
  => BlockEra
  -> ShelleyBlock (TPraos c) era
  -> BlockPrecis
getTPraosBlockPrecis era block =
  let (blockHeaderBody, blockHeaderSize) =
        case bheader (shelleyBlockRaw block) of
          bh@(BHeader bhBody _) -> (bhBody, bHeaderSize bh)
      (txsSeq, blockSize) = case shelleyBlockRaw block of
        Block' _ txs bs -> (fromTxSeq txs, fromIntegral (BSL.length bs))
      blockBodySize = fromIntegral (bsize blockHeaderBody)
   in assert (blockSize == blockHeaderSize + blockBodySize) $
        BlockPrecis
          { bpEra = era
          , bpSlotNo = bheaderSlotNo blockHeaderBody
          , bpBlockNo = bheaderBlockNo blockHeaderBody
          , -- , bpAbsBlockNo = absBlockNo
            bpProtVer = Just $! bprotver blockHeaderBody
          , bpBlockSize = blockSize
          , bpBlockBodySize = blockBodySize
          , bpBlockHeaderSize = blockHeaderSize
          , bpTxsPrecis = getTxsPrecis txsSeq
          }

getPraosBlockPrecis
  :: (EraSegWits era, Crypto c)
  => BlockEra
  -> ShelleyBlock (Praos c) era
  -> BlockPrecis
getPraosBlockPrecis era block =
  let blockHeader = bheader (shelleyBlockRaw block)
      blockHeaderBody = headerBody blockHeader
      blockHeaderSize = headerSize blockHeader
      (txsSeq, blockSize) = case shelleyBlockRaw block of
        Block' _ txs bs -> (fromTxSeq txs, fromIntegral (BSL.length bs))
      blockBodySize = fromIntegral (hbBodySize blockHeaderBody)
   in assert (blockSize == blockHeaderSize + blockBodySize) $
        BlockPrecis
          { bpEra = era
          , bpSlotNo = hbSlotNo blockHeaderBody
          , bpBlockNo = hbBlockNo blockHeaderBody
          , -- , bpAbsBlockNo = absBlockNo
            bpProtVer = Just $ hbProtVer blockHeaderBody
          , bpBlockSize = blockSize
          , bpBlockBodySize = blockBodySize
          , bpBlockHeaderSize = blockHeaderSize
          , bpTxsPrecis = getTxsPrecis txsSeq
          }

getTxsPrecis :: (EraTx era, Foldable t) => t (Tx era) -> Vector TxPrecis
getTxsPrecis txsSeq =
  V.fromList $! getTxPrecis <$!> toList txsSeq

getTxPrecis :: EraTx era => Tx era -> TxPrecis
getTxPrecis tx =
  TxPrecis
    { tpSize = fromInteger (tx ^. sizeTxF)
    , tpInsCount = fromIntegral $ length $ tx ^. bodyTxL . inputsTxBodyL
    , tpOutsCount = fromIntegral $ length $ tx ^. bodyTxL . outputsTxBodyL
    }

applyBlockTxs
  :: forall a
   . ([B.ATxAux ByteString] -> a)
  -> ( forall era
        . (EraSegWits era, EraCrypto era ~ StandardCrypto)
       => [Tx era]
       -> a
     )
  -> CardanoBlock StandardCrypto
  -> a
applyBlockTxs applyByronTxs applyNonByronTxs =
  applyBlock applyByronBlock applyNonByronBlock applyNonByronBlock
  where
    applyByronBlock byronBlock =
      case byronBlockRaw byronBlock of
        B.ABOBBlock abBlock ->
          let B.ATxPayload atxs = B.bodyTxPayload (B.blockBody abBlock)
           in applyByronTxs atxs
        B.ABOBBoundary _abBlock -> applyByronTxs []
    applyNonByronBlock
      :: forall era p
       . (EraSegWits era, EraCrypto era ~ StandardCrypto)
      => BlockEra
      -> ShelleyBlock (p (EraCrypto era)) era
      -> a
    applyNonByronBlock _ = applyNonByronTxs . toList . fromTxSeq . bbody . shelleyBlockRaw

getSlotNo :: CardanoBlock StandardCrypto -> SlotNo
getSlotNo =
  applyBlock
    byronBlockSlotNo
    (\_ -> bheaderSlotNo . getTPraosBHeaderBody)
    (\_ -> hbSlotNo . getPraosBHeaderBody)

getSlotNoWithEra :: CardanoBlock StandardCrypto -> (BlockEra, SlotNo)
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
  :: Set (Credential 'Staking StandardCrypto)
  -> CardanoBlock StandardCrypto
  -> Map (Credential 'Staking StandardCrypto) Coin
filterBlockWithdrawals creds =
  applyBlockTxs (const mempty) $ \txs ->
    Map.unionsWith (<+>) $
      ( map
          ( \tx ->
              let wdrls = Map.mapKeys getRwdCred $ unWithdrawals (tx ^. bodyTxL . withdrawalsTxBodyL)
               in wdrls `Map.restrictKeys` creds
          )
          txs
      )
