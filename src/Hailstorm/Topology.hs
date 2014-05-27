module Hailstorm.Topology
( Topology(..)
, HardcodedTopology(..)
, spoutIds
) where

import Data.Maybe
import Hailstorm.Payload
import Hailstorm.Processor
import qualified Data.Map as Map

class Topology t where

    processors :: t
               -> Map.Map String Processor

    downstreamAddresses :: t
                        -> ProcessorName
                        -> Payload k v
                        -> [ProcessorAddress]

    addressFor :: t
               -> ProcessorId
               -> ProcessorAddress

    numProcessors :: t
                  -> Int

    upstreamProcessorNames :: t
                           -> ProcessorName
                           -> [ProcessorName]

type ProcessorHost = String
type ProcessorPort = String
type ProcessorAddress = (ProcessorHost, ProcessorPort)

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

spoutIds :: (Topology t) => t -> [ProcessorId]
spoutIds t = [(n,c)
             | (_, Processor Spout n p _) <- Map.toList (processors t)
             , c <- [0..(p - 1)]]
