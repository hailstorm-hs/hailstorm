module Hailstorm.Topology
( Topology(..)
, HardcodedTopology(..)
, ProcessorThreadIndex
) where

import Data.Maybe
import Hailstorm.Payload
import Hailstorm.Processor
import qualified Data.Map as Map

class Topology t where
    processors :: t
               -> Map.Map String Processor
    downstreamAddresses :: t
                        -> String
                        -> Payload k v
                        -> [ProcessorAddress]
    addressFor :: t
               -> ProcessorThreadIndex
               -> ProcessorAddress
    numProcessors :: t
                  -> Int

type ProcessorThreadIndex = (String, Int)
type ProcessorHost = String
type ProcessorPort = String
type ProcessorAddress = (ProcessorHost, ProcessorPort)

data HardcodedTopology = HardcodedTopology
    { processorMap :: Map.Map String Processor
    , addresses :: Map.Map ProcessorThreadIndex ProcessorAddress
    } deriving (Eq, Show, Read)

instance Topology HardcodedTopology where
    downstreamAddresses t processorName _ =
        let upstream = fromJust $ Map.lookup processorName (processorMap t)
            findAddress downstreamName =
                let downstream = fromJust $
                        Map.lookup downstreamName (processorMap t)
                in fromJust $ Map.lookup
                    (name downstream, parallelism downstream - 1) (addresses t)
        in map findAddress (downstreams upstream)

    addressFor t (processorName, processorNumber) = fromJust $
        Map.lookup (processorName, processorNumber) (addresses t)

    numProcessors (HardcodedTopology pmap _) = Map.fold (\p l -> l + case p of
            (Spout _ ps _) -> ps
            (Bolt _ ps _) -> ps
            (Sink _ ps) -> ps
        ) 0 pmap

    processors = processorMap
