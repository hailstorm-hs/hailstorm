module Hailstorm.Processor
( Processor(..)
, ProcessorName
, ProcessorInstance
, ProcessorId
, mkProcessorMap
) where

import qualified Data.Map as Map

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
