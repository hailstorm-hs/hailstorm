{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.Processor
( Processor(..)
, ProcessorNode(..)
, Spout(..)
, Bolt(..)
, Sink(..)
, ProcessorName
, ProcessorInstance
, ProcessorId
, processorIds
) where

import Control.Exception
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.TransactionTypes
import qualified Data.ByteString as BS

type ProcessorName = String
type ProcessorInstance = Int
type ProcessorId = (ProcessorName, ProcessorInstance)

class Processor p where
    processorName :: p -> ProcessorName
    parallelism :: p -> Int
    serializer :: p -> PayloadTuple -> String
    deserializer :: p -> String -> PayloadTuple

data ProcessorNode = SpoutNode Spout
                   | BoltNode Bolt
                   | SinkNode Sink

data Spout =
    Spout { spoutName :: String
          , spoutPartitions :: [Partition]
          , convertFn :: BS.ByteString -> PayloadTuple
          , spoutSerializer :: PayloadTuple -> String
          }

data Bolt =
    Bolt { boltName :: String
         , boltParallelism :: Int
         , upstreamDeserializer :: String -> PayloadTuple
         , downstreamSerializer :: PayloadTuple -> String
         , tupleToStateConverter :: PayloadTuple -> BoltState
         , stateSerializer :: BoltState -> String
         , stateDeserializer :: String -> BoltState
         , emptyState :: BoltState
         , mergeFn :: BoltState -> BoltState -> BoltState
         , transformTupleFn :: PayloadTuple -> BoltState -> PayloadTuple
         }

data Sink =
    Sink { sinkName :: String
         , sinkParallelism :: Int
         , outputFn :: PayloadTuple -> IO ()
         , sinkDeserializer :: String -> PayloadTuple
         }

instance Processor ProcessorNode where

    processorName (SpoutNode s) = spoutName s
    processorName (BoltNode b) = boltName b
    processorName (SinkNode k) = sinkName k

    parallelism (SpoutNode s) = length $ spoutPartitions s
    parallelism (BoltNode b) = boltParallelism b
    parallelism (SinkNode k) = sinkParallelism k

    deserializer (SpoutNode s) = throw $ InvalidTopologyError $
        "no deserializer exists for spout " ++ spoutName s
    deserializer (BoltNode b) = upstreamDeserializer b
    deserializer (SinkNode k) = sinkDeserializer k

    serializer (SpoutNode s) = spoutSerializer s
    serializer (BoltNode b) = downstreamSerializer b
    serializer (SinkNode k) = throw $ InvalidTopologyError $
        "no serializer exists for sink " ++ sinkName k

processorIds :: (Processor p) => p -> [ProcessorId]
processorIds pr = [(processorName pr, i) | i <- [0..parallelism pr]]
