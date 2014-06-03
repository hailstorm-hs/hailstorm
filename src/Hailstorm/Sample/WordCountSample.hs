{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Sample.WordCountSample
( wordCountTopology
) where

import Control.Exception
import Data.Dynamic
import Data.List
import Data.Maybe
import Data.Monoid
import Data.Ord
import Hailstorm.Error
import Hailstorm.Topology.HardcodedTopology
import Hailstorm.Processor
import Hailstorm.TransactionTypes
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

localServer :: String
localServer = "127.0.0.1"

data TypeableIntSum = TIS (Sum Int)
                      deriving (Eq, Typeable, Show, Read)

instance Ord TypeableIntSum where
   (TIS (Sum a)) `compare` (TIS (Sum b)) = a `compare` b

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

dynToListTuple :: Dynamic -> [(String, TypeableIntSum)]
dynToListTuple d = flip fromMaybe (fromDynamic d) $
    throw $ InvalidTopologyError "Unexpected value: not a [(word, count)] tuple"

dynToMMWrapper :: Dynamic -> MonoidMapWrapper String TypeableIntSum
dynToMMWrapper d = flip fromMaybe (fromDynamic d) $
    throw $ InvalidTopologyError "Unexpected value: not a word-count) state map"

tupleToState :: PayloadTuple -> BoltState
tupleToState tup = let (key, val) = payloadTupleToWordCountTuple tup
                   in MkBoltState $ toDyn $ MonoidMapWrapper (Map.singleton key val)

payloadTupleToWordCountTuple :: PayloadTuple -> (String, TypeableIntSum)
payloadTupleToWordCountTuple (MkPayloadTuple d) = dynToWordCountTuple d

payloadTupleToListTuple :: PayloadTuple -> [(String, TypeableIntSum)]
payloadTupleToListTuple (MkPayloadTuple d) = dynToListTuple d

readTISPayloadTuple :: String -> PayloadTuple
readTISPayloadTuple x = MkPayloadTuple $ toDyn (read x :: (String, TypeableIntSum))

readListPayloadTuple :: String -> PayloadTuple
readListPayloadTuple x = MkPayloadTuple $ toDyn (read x :: [(String, TypeableIntSum)])

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
        v = Map.findWithDefault
            (error $ "Could not find " ++ k ++ " in state " ++ show m) k m
    in (MkPayloadTuple $ toDyn (k, v))

outputBoltState :: PayloadTuple -> BoltState -> PayloadTuple
outputBoltState _ (MkBoltState dynM) = 
    let MonoidMapWrapper m = dynToMMWrapper dynM
        sorted = sortBy (comparing $ snd) (Map.toList m)
    in (MkPayloadTuple $ toDyn sorted)

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
    , upstreamDeserializer = readTISPayloadTuple
    , downstreamSerializer = show . payloadTupleToWordCountTuple
    , stateDeserializer = readBoltState
    , stateSerializer = show . boltStateToInnerMap
    , tupleToStateConverter = tupleToState
    , emptyState = MkBoltState $ toDyn (mempty :: MonoidMapWrapper String TypeableIntSum)
    , mergeFn = mergeStates
    , transformTupleFn = lookupTupleInState
    }

sortBolt :: Bolt
sortBolt = Bolt
    { boltName = "sort"
    , boltParallelism = 1
    , upstreamDeserializer = readTISPayloadTuple
    , downstreamSerializer = show . payloadTupleToListTuple
    , stateDeserializer = readBoltState
    , stateSerializer = show . boltStateToInnerMap
    , tupleToStateConverter = tupleToState
    , emptyState = MkBoltState $ toDyn (mempty :: MonoidMapWrapper String TypeableIntSum)
    , mergeFn = mergeStates
    , transformTupleFn = outputBoltState
    }

outputSink :: Sink
outputSink = Sink
    { sinkName = "sink"
    , sinkParallelism = 1
    --, outputFn = \_ -> return ()
    , outputFn = \x -> print (payloadTupleToListTuple x) 
    , sinkDeserializer = readListPayloadTuple
    }

wordCountTopology :: HardcodedTopology
wordCountTopology = HardcodedTopology
  {
      processorNodeMap = mkProcessorMap
      [ SpoutNode wordsSpout
      , BoltNode countBolt
      , BoltNode sortBolt
      , SinkNode outputSink
      ]
      ,
      downstreamMap = Map.fromList
      [ ("words", [("count", const 0)]) -- TODO: implement grouping function
      , ("count", [("sort", const 0)]) -- TODO: implement grouping function
      , ("sort", [("sink", const 0)]) -- TODO: implement grouping function
      ]
      ,
      addresses = Map.fromList
      [ (("sink", 0), (localServer, "10000"))
      , (("count", 0), (localServer, "10001"))
      , (("sort", 0), (localServer, "10002"))
      ]
  }
