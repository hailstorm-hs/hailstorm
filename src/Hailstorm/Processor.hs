module Hailstorm.Processor
( Processor(..)
, mkProcessorMap
) where

import qualified Data.Map as Map

type ProcessorId = String
data Processor = Spout { name :: ProcessorId
                       , parallelism :: Int
                       , downstreams :: [ProcessorId]
                       }
               | Bolt  { name :: ProcessorId
                       , parallelism :: Int
                       , downstremas :: [ProcessorId]
                       }
               | Sink  { name :: ProcessorId
                       , parallelism :: Int
                       }
                 deriving (Eq, Show, Read)

mkProcessorMap :: [Processor] -> Map.Map ProcessorId Processor
mkProcessorMap = Map.fromList . map (\x -> (name x, x))
