{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Cardano.Streamer.BlockInfo where

import qualified Cardano.Chain.Block as B
import qualified Cardano.Chain.UTxO as B
import qualified Cardano.Chain.Update as B
import Cardano.Ledger.BaseTypes
import Cardano.Ledger.Block
import Cardano.Ledger.Crypto
import Cardano.Ledger.Hashes
import Cardano.Ledger.SafeHash
import Cardano.Protocol.TPraos.BHeader
import Cardano.Streamer.Common
import Control.Monad.Trans.Fail.String (errorFail)
import qualified Data.ByteString as BS
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
  , tpInCount :: !Int16
  , tpOutCount :: !Int16
  }

data BlockPrecis = BlockPrecis
  { bpEra :: !BlockEra
  -- ^ Current epoch
  , bpEpochNo :: !EpochNo
  -- ^ Current epoch number.
  --
  -- In Byron a boundary block changes this value, otherwise it will be inferred from the
  -- ledger state
  , bpSlotNo :: !SlotNo
  , bpBlockNo :: !BlockNo
  , bpAbsBlockNo :: !Word64
  , bpProtVer :: !ProtVer
  , bpBlockSize :: !Int
  , bpBlockBodySize :: !Int
  , bpBlockHeaderSize :: !Int
  , bpTxsPrecis :: !(V.Vector TxPrecis)
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

getRawBlock :: CardanoBlock StandardCrypto -> RawBlock StandardCrypto
getRawBlock =
  mkRawBlock . \case
    BlockByron byronBlock ->
      case byronBlockRaw byronBlock of
        B.ABOBBlock abBlock -> B.blockAnnotation abBlock
        B.ABOBBoundary abBlock -> B.boundaryAnnotation abBlock
    BlockShelley shelleyBlock -> blockBytes shelleyBlock
    BlockAllegra allegraBlock -> blockBytes allegraBlock
    BlockMary maryBlock -> blockBytes maryBlock
    BlockAlonzo alonzoBlock -> blockBytes alonzoBlock
    BlockBabbage babbageBlock -> blockBytes babbageBlock
    BlockConway conwayBlock -> blockBytes conwayBlock
  where
    blockBytes block =
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

-- getBlockInfo :: ProtVer -> EpochNo -> Word64 -> CardanoBlock StandardCrypto -> BlockPrecis
-- getBlockInfo protVer epochNo absBlockNo = \case
--   BlockByron byronBlock ->
--     case byronBlockRaw byronBlock of
--       B.ABOBBlock abBlock ->
--         let bbHeader = B.blockHeader abBlock
--             bbBody = B.blockBody abBlock
--             bbBlockSize = BS.length (B.blockAnnotation abBlock)
--             bbHeaderSize = BS.length (B.headerAnnotation bbHeader)
--             B.ATxPayload atxs = B.bodyTxPayload bbBody
--             byronATxAuxPrecis atx =
--               let tx = B.taTx atx
--                in TxPrecis
--                     { tpSize = fromIntegral $ BS.length (B.aTaAnnotation atx)
--                     , tpInCount = fromIntegral $ length (B.txInputs tx)
--                     , tpOutCount = fromIntegral $ length (B.txOutputs tx)
--                     }
--             bProtVer = fromByronProtocolVersion (B.headerProtocolVersion bbHeader)
--          in assert (protVer == bProtVer) $
--               BlockPrecis
--                 { bpEra = Byron
--                 , bpEpochNo = epochNo
--                 , bpSlotNo = byronBlockSlotNo byronBlock
--                 , bpBlockNo = 0 -- TODO: compute from bpAbsBlockNo and slotNo
--                 , bpAbsBlockNo = absBlockNo
--                 , bpProtVer = bProtVer
--                 , bpBlockSize = bbBlockSize
--                 , bpBlockBodySize = bbBlockSize - bbHeaderSize
--                 , bpBlockHeaderSize = bbHeaderSize
--                 , bpTxsPrecis = V.fromList (byronATxAuxPrecis <$!> atxs)
--                 }
--       B.ABOBBoundary abBlock ->
--         let bbHeader = B.boundaryHeader abBlock
--             bbBody = B.boundaryBody abBlock
--             bbHeaderSize = BS.length (B.boundaryHeaderAnnotation bbHeader)
--          in BlockPrecis
--               { bpEra = Byron
--               , bpEpochNo = EpochNo (B.boundaryEpoch bbHeader)
--               , bpSlotNo = byronBlockSlotNo byronBlock
--               , bpBlockNo = 0 -- TODO: compute from absBlockNo and slotNo
--               , bpAbsBlockNo = absBlockNo
--               , bpProtVer = protVer
--               , bpBlockSize = fromIntegral (B.boundaryBlockLength abBlock) --
--               , bpBlockBodySize = BS.length (B.boundaryBodyAnnotation bbBody)
--               , bpBlockHeaderSize = bbHeaderSize
--               , bpTxsPrecis = V.empty
--               }
--   BlockShelley shelleyBlock ->
--     let bHeader = getTPraosBHeader shelleyBlock
--         bHeaderBody = bhbody bHeaderBody
--         bSize = case shelleyBlockRaw shelleyBlock of
--           Block' _ txs bs -> fromIntegral (BSL.length bs)
--         bBodySize = fromIntegral (bsize bHeaderBody)
--         nHeaderSize = BS.length (bHeaderBytes bHeaderBody)
--      in assert (bSize == bHeaderSize + bBodySize) $
--           BlockPrecis
--             { bpEra = Shelley
--             , bpEpochNo = epochNo
--             , bpSlotNo = bheaderSlotNo bHeaderBody
--             , bpBlockNo = bheaderBlockNo bHeaderBody
--             , bpAbsBlockNo = absBlockNo
--             , bpProtVer = bprotver bHeaderBody
--             , bpBlockSize = bSize
--             , bpBlockBodySize = bBodySize
--             , bpBlockHeaderSize = bHeaderSize
--             , bpTxsPrecis = V.fromList $ map shelleyTxPrecis $ toList $ fromTxSeq txs
--             }
--   where
--     shelleyTxPrecis tx =
--       TxPrecis
--         { tpIns = length $ tx ^. bodyTxL . inputsTxBodyL
--         , tpOuts = length $ tx ^. bodyTxL . outputTxBodyL
--         }

-- BlockBabbage shelleyBlock ->
--   let bHeader = getTPraosBHeader shelleyBlock
--    in BlockPrecis
--         { bpEra = Babbage
--         , bpEpochNo = epochNo
--         , bpSlotNo = bheaderSlotNo bHeader
--         , bpBlockNo = hbBlockNo bHeader
--         , bpAbsBlockNo = absBlockNo
--         , bpProtVer = undefined
--         , bpBlockSize = undefined
--         , bpBlockBodySize = undefined
--         , bpBlockHeaderSize = undefined
--         , bpTxPrecis = undefined
--         }

-- BlockAllegra allegraBlock -> bheaderSlotNo $ getTPraosBHeader allegraBlock
-- BlockMary maryBlock -> bheaderSlotNo $ getTPraosBHeader maryBlock
-- BlockAlonzo alonzoBlock -> bheaderSlotNo $ getTPraosBHeader alonzoBlock
-- BlockBabbage babbageBlock -> hbSlotNo $ getPraosBHeader babbageBlock
-- BlockConway conwayBlock -> hbSlotNo $ getPraosBHeader conwayBlock

getSlotNo :: CardanoBlock StandardCrypto -> SlotNo
getSlotNo = \case
  BlockByron byronBlock -> byronBlockSlotNo byronBlock
  BlockShelley shelleyBlock -> bheaderSlotNo $ getTPraosBHeader shelleyBlock
  BlockAllegra allegraBlock -> bheaderSlotNo $ getTPraosBHeader allegraBlock
  BlockMary maryBlock -> bheaderSlotNo $ getTPraosBHeader maryBlock
  BlockAlonzo alonzoBlock -> bheaderSlotNo $ getTPraosBHeader alonzoBlock
  BlockBabbage babbageBlock -> hbSlotNo $ getPraosBHeader babbageBlock
  BlockConway conwayBlock -> hbSlotNo $ getPraosBHeader conwayBlock

getTPraosBHeader :: Crypto c => ShelleyBlock (TPraos c) era -> BHBody c
getTPraosBHeader block =
  case bheader (shelleyBlockRaw block) of
    BHeader bhBody _ -> bhBody

getPraosBHeader :: Crypto c => ShelleyBlock (Praos c) era -> HeaderBody c
getPraosBHeader block = headerBody (bheader (shelleyBlockRaw block))
