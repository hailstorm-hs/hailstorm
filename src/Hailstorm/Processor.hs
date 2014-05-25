module Hailstorm.Processor
( Processor(..)
, ProcessorState(..)
, ProcessorName
, ProcessorInstance
, ProcessorId
, mkProcessorMap
) where

import Hailstorm.Partition
import qualified Data.Map as Map

data ProcessorState = 
      BoltRunning
    | SinkRunning
    | SpoutPaused Partition Offset
    | SpoutRunning
    | UnspecifiedState
    deriving (Eq, Show, Read)

type ProcessorInstance = Int
type ProcessorName = String
type ProcessorId = (ProcessorName, ProcessorInstance)

data Processor = Spout { name :: ProcessorName
                       , parallelism :: Int
                       , downstreams :: [ProcessorName]
                       }
               | Bolt  { name :: ProcessorName
                       , parallelism :: Int
                       , downstreams :: [ProcessorName]
                       }
               | Sink  { name :: ProcessorName
                       , parallelism :: Int
                       }
                 deriving (Eq, Show, Read)

mkProcessorMap :: [Processor] -> Map.Map ProcessorName Processor
mkProcessorMap = Map.fromList . map (\x -> (name x, x))
