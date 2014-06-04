module Hailstorm.Topology
( Topology(..)
, ProcessorAddress
) where

import Hailstorm.Payload
import Hailstorm.Processor

type ProcessorHost = String
type ProcessorPort = String
type ProcessorAddress = (ProcessorHost, ProcessorPort)

class Topology t where

    -- | Returns a list of processor nodes (of various types).
    spouts :: t -> [ProcessorNode]
    bolts  :: t -> [ProcessorNode]
    sinks  :: t -> [ProcessorNode]

    lookupProcessor :: ProcessorName -> t -> Maybe ProcessorNode

    lookupProcessorWithFailure :: ProcessorName -> t -> ProcessorNode

    downstreamAddresses :: t
                        -> ProcessorName
                        -> Payload
                        -> [ProcessorAddress]

    addressFor :: t -> ProcessorId -> ProcessorAddress

    numProcessors :: t -> Int
