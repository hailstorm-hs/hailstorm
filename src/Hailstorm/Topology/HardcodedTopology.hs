module Hailstorm.Topology.HardcodedTopology
( HardcodedTopology(..)
) where

import Data.Maybe
import Hailstorm.Processor
import Hailstorm.Topology
import qualified Data.Map as Map

data HardcodedTopology = HardcodedTopology
    { processorMap :: Map.Map String Processor
    , addresses :: Map.Map ProcessorId ProcessorAddress
    } deriving (Eq, Show, Read)

instance Topology HardcodedTopology where

    downstreamAddresses t processorName _ =
        -- TODO: hash tuple value to determine which instance to deliver to
        let upstream = fromJust $ Map.lookup processorName (processorMap t)
            findAddress downstreamName =
                let downstream = fromJust $
                        Map.lookup downstreamName (processorMap t)
                in fromJust $ Map.lookup
                    (name downstream, parallelism downstream - 1) (addresses t)
        in map findAddress (downstreams upstream)

    addressFor t (processorName, processorNumber) = fromJust $
        Map.lookup (processorName, processorNumber) (addresses t)

    numProcessors (HardcodedTopology pmap _) =
        Map.fold (\p l -> l + parallelism p) 0 pmap

    processors = processorMap

    upstreamProcessorNames t pName =
        let lookupInDownstreams p = if pName `elem` downstreams p
                                        then Just (name p)
                                        else Nothing
        in mapMaybe lookupInDownstreams $ Map.elems (processorMap t)
