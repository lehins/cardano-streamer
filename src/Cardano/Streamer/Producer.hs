{-# LANGUAGE LambdaCase #-}

module Cardano.Streamer.Producer where

import Cardano.Binary
import Cardano.Streamer.Common

import Conduit
import Control.Monad.Trans.Except
import Ouroboros.Consensus.Block
import Ouroboros.Consensus.Config
import qualified Ouroboros.Consensus.Fragment.InFuture as InFuture
import Ouroboros.Consensus.Ledger.Extended
import qualified Ouroboros.Consensus.Node as Node
import Ouroboros.Consensus.Storage.ChainDB as ChainDB
import qualified Ouroboros.Consensus.Storage.ImmutableDB as ImmutableDB
import Ouroboros.Consensus.Storage.LedgerDB
import Ouroboros.Consensus.Storage.Serialisation
import qualified Ouroboros.Consensus.Storage.VolatileDB as VolatileDB
import Ouroboros.Consensus.Util.ResourceRegistry
import UnliftIO.Exception


-- sourceImmutableDb startDiskSnapshot = do

