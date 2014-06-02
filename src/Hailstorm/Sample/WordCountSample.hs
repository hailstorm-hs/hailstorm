{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Sample.WordCountSample
( wordCountTopology
) where

import Control.Exception
import Data.Dynamic
import Data.Maybe
import Data.Monoid
import Hailstorm.Error
import Hailstorm.Topology.HardcodedTopology
import Hailstorm.Processor
import Hailstorm.TransactionTypes
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

localServer :: String
localServer = "127.0.0.1"

data TypeableIntSum = TIS (Sum Int)
                      deriving (Typeable, Show, Read)

instance Monoid TypeableIntSum where
    mempty = TIS (mempty :: (Sum Int))
    (TIS x) `mappend` (TIS y) = TIS (x `mappend` y)


data MonoidMapWrapper k v = (Ord k, Monoid v) => MonoidMapWrapper (Map.Map k v)
                            deriving (Typeable)
instance (Ord k, Monoid v) => Monoid (MonoidMapWrapper k v) where
    mempty = MonoidMapWrapper Map.empty
    (MonoidMapWrapper m) `mappend` (MonoidMapWrapper n) = MonoidMapWrapper $
        Map.unionWith mappend m n

dynToWordCountTuple :: Dynamic -> (String, TypeableIntSum)
dynToWordCountTuple d = flip fromMaybe (fromDynamic d) $
    throw $ InvalidTopologyError "Unexpected value: not a (word, count) tuple"

dynToMMWrapper :: Dynamic -> MonoidMapWrapper String TypeableIntSum
dynToMMWrapper d = flip fromMaybe (fromDynamic d) $
    throw $ InvalidTopologyError "Unexpected value: not a word-count) state map"

tupleToState :: PayloadTuple -> BoltState
tupleToState tup = let (key, val) = payloadTupleToWordCountTuple tup
                   in MkBoltState $ toDyn $ MonoidMapWrapper (Map.singleton key val)

payloadTupleToWordCountTuple :: PayloadTuple -> (String, TypeableIntSum)
payloadTupleToWordCountTuple (MkPayloadTuple d) = dynToWordCountTuple d

readPayloadTuple :: String -> PayloadTuple
readPayloadTuple x = MkPayloadTuple $ toDyn (read x :: (String, TypeableIntSum))

boltStateToInnerMap :: BoltState -> Map.Map String TypeableIntSum
boltStateToInnerMap (MkBoltState d) = let (MonoidMapWrapper m') = dynToMMWrapper d
                                      in m'

readBoltState :: String -> BoltState
readBoltState x = MkBoltState $ toDyn $
    MonoidMapWrapper (read x :: Map.Map String TypeableIntSum)

mergeStates :: BoltState -> BoltState -> BoltState
mergeStates (MkBoltState dynM) (MkBoltState dynN) =
    let m' = dynToMMWrapper dynM
        n' = dynToMMWrapper dynN
        mr = m' `mappend` n'
    in MkBoltState $ toDyn mr

lookupTupleInState :: PayloadTuple -> BoltState -> PayloadTuple
lookupTupleInState tup (MkBoltState dynM) =
    let MonoidMapWrapper m = dynToMMWrapper dynM
        (k, _) = payloadTupleToWordCountTuple tup
        v = m Map.! k
    in (MkPayloadTuple $ toDyn (k, v))

wordsSpout :: Spout
wordsSpout = Spout
    { spoutName = "words"
    , spoutPartitions = ["all_words"]
    , convertFn = \x -> MkPayloadTuple $ toDyn (C8.unpack x, TIS (Sum 1))
    , spoutSerializer = show . payloadTupleToWordCountTuple
    }

countBolt :: Bolt
countBolt = Bolt
    { boltName = "count"
    , boltParallelism = 1
    , upstreamDeserializer = readPayloadTuple
    , downstreamSerializer = show . payloadTupleToWordCountTuple
    , stateDeserializer = readBoltState
    , stateSerializer = show . boltStateToInnerMap
    , tupleToStateConverter = tupleToState
    , emptyState = MkBoltState $ toDyn (mempty :: MonoidMapWrapper String TypeableIntSum)
    , mergeFn = mergeStates
    , transformTupleFn = lookupTupleInState
    }

outputSink :: Sink
outputSink = Sink
    { sinkName = "sink"
    , sinkParallelism = 1
    , outputFn = \_ -> return ()
    , sinkDeserializer = readPayloadTuple
    }

wordCountTopology :: HardcodedTopology
wordCountTopology = HardcodedTopology
  {
      processorNodeMap = mkProcessorMap
      [ SpoutNode wordsSpout
      , BoltNode countBolt
      , SinkNode outputSink
      ]
      ,
      downstreamMap = Map.fromList
      [ ("words", [("count", const 0)]) -- TODO: implement grouping function
      , ("count", [("sink", const 0)]) -- TODO: implement grouping function
      ]
      ,
      addresses = Map.fromList
      [ (("sink", 0), (localServer, "10000"))
      , (("count", 0), (localServer, "10001"))
      ]
  }
