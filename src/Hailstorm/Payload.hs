module Hailstorm.Payload
( Payload(..)
, partitionToPayloadProducer
) where

import Pipes
import Hailstorm.Clock
import Hailstorm.UserFormula
import Hailstorm.Partition
import qualified Data.Map as Map

data Payload k v = Payload
    { payloadTuple :: (k,v)
    , payloadClock :: Clock
    } deriving (Eq, Show, Read)

partitionToPayloadProducer :: (Show k, Show v, Monad m)
                           => UserFormula k v
                           -> Producer PartitionOffset m ()
                           -> Producer (Payload k v) m ()
partitionToPayloadProducer uformula producer =
    let processProducer x =
            case x of
                PartitionOffset bs p o -> yield $ Payload
                    (convertFn uformula bs) (Clock $ Map.singleton p o)
    in for producer processProducer

