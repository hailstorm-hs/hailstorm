module Hailstorm.Topology
( Topology(..)
, ProcessorAddress
, spoutIds
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

spoutIds :: (Topology t) => t -> [ProcessorId]
spoutIds t = [(n,c)
             | (_, Processor Spout n p _) <- Map.toList (processors t)
             , c <- [0..(p - 1)]]
