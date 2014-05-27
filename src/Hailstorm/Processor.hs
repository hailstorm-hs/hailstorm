module Hailstorm.Processor
( Processor(..)
, ProcessorType(..)
, ProcessorName
, ProcessorInstance
, ProcessorId
, mkProcessorMap
) where

import qualified Data.Map as Map

type ProcessorInstance = Int
type ProcessorName = String
type ProcessorId = (ProcessorName, ProcessorInstance)

data ProcessorType = Spout | Bolt | Sink
                     deriving (Eq, Show, Read)

data Processor = Processor { processorType :: ProcessorType
                           , name :: ProcessorName
                           , parallelism :: Int
                           , downstreams :: [ProcessorName]
                           } deriving (Eq, Show, Read)

mkProcessorMap :: [Processor] -> Map.Map ProcessorName Processor
mkProcessorMap = Map.fromList . map (\x -> (name x, x))
