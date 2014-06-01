{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.ProcessorNew
where

import Data.Monoid
import Hailstorm.Clock
import qualified Data.ByteString as BS

-- type ProcessorInstance = Int
type ProcessorName = String
-- type ProcessorId = (ProcessorName, ProcessorInstance)

class Processor p where
    processorName :: p -> ProcessorName
    parallelism :: p -> Int
    -- downstreams :: p -> [ProcessorName]
    -- downstreamSerialize :: p -> a -> BS.ByteString
    -- upstreamDeserialize :: p -> BS.ByteString -> a

data Spout a b = Spout { spoutName :: String
                       , spoutPartitions :: [Partition]
                       , convertFun :: BS.ByteString -> a
                       , spoutSerializer :: b -> BS.ByteString
                       }

instance Processor (Spout a b) where
    processorName = spoutName
    parallelism = length . spoutPartitions

data Bolt a b = (Monoid a) => Bolt { boltName :: String
                       , boltParallelism :: Int
                       , upstreamDeserializer :: BS.ByteString -> a
                       , downstreamSerializer :: b -> BS.ByteString
                       , computeFun :: a -> a -> (b, a)
                       }

instance Processor (Bolt a b) where
    processorName = boltName
    parallelism = boltParallelism

data Sink a = Sink { sinkName :: String
                   , sinkParallelism :: Int
                   , outputFun :: a -> IO ()
                   , sinkDeserializer :: BS.ByteString -> a
                   }

instance Processor (Sink a) where
    processorName = sinkName
    parallelism = sinkParallelism
