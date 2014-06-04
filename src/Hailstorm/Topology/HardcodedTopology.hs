module Hailstorm.Topology.HardcodedTopology
( HardcodedTopology(..)
, mkProcessorMap
, GroupingFn
) where

import Data.Maybe
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Topology
import Hailstorm.TransactionTypes
import qualified Data.Map as Map

type GroupingFn = (PayloadTuple -> Int)

data HardcodedTopology = HardcodedTopology
    { processorNodeMap :: Map.Map ProcessorName ProcessorNode
    , downstreamMap :: Map.Map ProcessorName [(ProcessorName, GroupingFn)]
    , addresses :: Map.Map ProcessorId ProcessorAddress
    }

instance Topology HardcodedTopology where

    spouts t = [ pr | pr@(SpoutNode _) <- Map.elems (processorNodeMap t) ]
    bolts t  = [ pr | pr@(BoltNode _)  <- Map.elems (processorNodeMap t) ]
    sinks t  = [ pr | pr@(SinkNode _)  <- Map.elems (processorNodeMap t) ]

    downstreamAddresses t pName payload =
        let downstreams = Map.findWithDefault (error $ "Could not find " ++
                show pName ++ " in topology " ++ show (Map.keys $ downstreamMap t))
                pName (downstreamMap t)
            findTargetInstance groupFn par =
                groupFn (payloadTuple payload) `mod` par
            findAddress (downstreamName, groupFn) =
                let downstream = lookupProcessorWithFailure pName t
                    queryId = (downstreamName, findTargetInstance groupFn
                                (parallelism downstream))
                in fromMaybe (error $ "Could not find " ++ show queryId ++
                    " in addresses " ++ show (Map.keys $ addresses t)) $
                    Map.lookup queryId (addresses t)
        in map findAddress downstreams

    lookupProcessor pName t = Map.lookup pName (processorNodeMap t)

    lookupProcessorWithFailure pName t =
        fromMaybe
            (error $ "Could not find " ++ show pName ++
                " in topology " ++ show (Map.keys $ processorNodeMap t))
            (lookupProcessor pName t)

    addressFor t pId = fromMaybe
        (error $ "Could not find " ++ show pId ++ " in " ++ show (Map.keys $ addresses t)) $
            Map.lookup pId (addresses t)

    numProcessors (HardcodedTopology pmap _ _) =
        Map.fold (\p l -> l + parallelism p) 0 pmap

mkProcessorMap :: Processor p => [p] -> Map.Map ProcessorName p
mkProcessorMap = Map.fromList . map (\x -> (processorName x, x))
