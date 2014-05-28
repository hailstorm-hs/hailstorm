module Hailstorm.Topology
( Topology(..)
, ProcessorAddress
, boltIds
, spoutIds
, processorIdsByType
) where

import Hailstorm.Payload
import Hailstorm.Processor
import qualified Data.Map as Map

type ProcessorHost = String
type ProcessorPort = String
type ProcessorAddress = (ProcessorHost, ProcessorPort)

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


processorIdsByType :: (Topology t) => t -> ProcessorType -> [ProcessorId]
processorIdsByType t desiredType  = 
             [(n,c)
             | (_, Processor pt n p _) <- Map.toList (processors t)
             , c <- [0..(p - 1)], pt == desiredType]

spoutIds :: (Topology t) => t -> [ProcessorId]
spoutIds t = processorIdsByType t Spout

boltIds :: (Topology t) => t -> [ProcessorId]
boltIds t = processorIdsByType t Bolt
