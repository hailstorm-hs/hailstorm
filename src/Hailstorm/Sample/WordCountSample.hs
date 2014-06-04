{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Sample.WordCountSample
( wordCountTopology
) where

import Control.Exception
import Control.Monad
import Data.Dynamic
import Data.Hashable
import Data.List
import Data.Maybe
import Data.Monoid
import Data.Ord
import Hailstorm.Error
import Hailstorm.Topology.HardcodedTopology
import Hailstorm.Processor
import Hailstorm.TransactionTypes
import Pipes
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map
import qualified Data.PSQueue as PS

localServer :: String
localServer = "127.0.0.1"

forceDyn :: Typeable a => Dynamic -> a
forceDyn x = case fromDynamic x of 
  Just y -> y
  Nothing -> throw $ InvalidTopologyError $ 
             "Word count sample forced dynamic to bad type: " ++ show x

deriving instance Typeable2 PS.PSQ

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


dynToMMWrapper :: Dynamic -> MonoidMapWrapper String TypeableIntSum
dynToMMWrapper d = flip fromMaybe (fromDynamic d) $
    throw $ InvalidTopologyError "Unexpected value: not a word-count) state map"

payloadTupleToWordCountTuple :: PayloadTuple -> (String, TypeableIntSum)
payloadTupleToWordCountTuple (MkPayloadTuple d) = dynToWordCountTuple d

readTISPayloadTuple :: String -> PayloadTuple
readTISPayloadTuple x = MkPayloadTuple $ toDyn (read x :: (String, TypeableIntSum))

wordsSpout :: Spout
wordsSpout = Spout
    { spoutName = "words"
    , spoutPartitions = ["all_words"]
    , convertFn = \x -> 
        MkPayloadTuple $ toDyn (C8.unpack x, TIS (Sum 1))
    , spoutSerializer = show . payloadTupleToWordCountTuple
    }

countBolt :: Bolt
countBolt = Bolt
    { boltName = "count"

    , boltParallelism = 2

    , upstreamDeserializer = readTISPayloadTuple

    , transformTupleFn = \tup (MkBoltState dynM) ->
        let MonoidMapWrapper m = dynToMMWrapper dynM
            (k, _) = payloadTupleToWordCountTuple tup
            v = Map.findWithDefault
                (error $ "Could not find " ++ k ++ " in state " ++ show m) k m
        in (MkPayloadTuple $ toDyn (k, v))

    , emptyState = 
        MkBoltState $ toDyn (mempty :: MonoidMapWrapper String TypeableIntSum)

    , mergeFn = \(MkBoltState dynM) (MkBoltState dynN) ->
        let m' = dynToMMWrapper dynM
            n' = dynToMMWrapper dynN
            mr = m' `mappend` n'
        in MkBoltState $ toDyn mr

    , tupleToStateConverter = \tup ->
        let (key, val) = payloadTupleToWordCountTuple tup
        in MkBoltState $ toDyn $ MonoidMapWrapper (Map.singleton key val)

    , downstreamSerializer = show . payloadTupleToWordCountTuple

    , stateDeserializer = \x -> MkBoltState $ toDyn $
        MonoidMapWrapper (read x :: Map.Map String TypeableIntSum)

    , stateSerializer = \(MkBoltState d) -> show $
        let (MonoidMapWrapper m') = dynToMMWrapper d in m'
    }



mergePSTopN :: Int -> PS.PSQ String Int -> PS.PSQ String Int -> PS.PSQ String Int
mergePSTopN n p1 p2 = 
  let (lesser, greater) = if PS.size p1 < PS.size p2 then (p1,p2) else (p2, p1)
      merged = foldr (\(k PS.:-> v) pq -> 
                        PS.alter (maybeMax v) k pq
                     ) greater (PS.toList lesser)
  in purgeN n merged
  where 
    maybeMax a (Just b) = Just $ max a b
    maybeMax a Nothing = Just $ a
    purgeN np pq = if (PS.size pq) <= n || np <= 0 then pq
                   else purgeN (np-1) (PS.deleteMin pq)


amtToSelect :: Int
amtToSelect = 3

topNBolt :: Bolt
topNBolt = Bolt
    { boltName = "topn"

    , boltParallelism = 2

    , upstreamDeserializer = readTISPayloadTuple

    , transformTupleFn = \_ (MkBoltState stateDyn) -> 
        let state = forceDyn $ stateDyn :: PS.PSQ String Int
        in MkPayloadTuple $ toDyn $ map (\(k PS.:-> v) -> (k,v)) (PS.toList state)

    , emptyState = 
        MkBoltState $ toDyn (PS.empty :: PS.PSQ String Int)

    , mergeFn = \(MkBoltState ps1Dyn) (MkBoltState ps2Dyn) ->
        let ps1 = forceDyn $ ps1Dyn :: PS.PSQ String Int
            ps2 = forceDyn $ ps2Dyn :: PS.PSQ String Int
        in MkBoltState $ toDyn $ mergePSTopN amtToSelect ps1 ps2

    , tupleToStateConverter = \tup ->
        let (key, TIS (Sum i)) = payloadTupleToWordCountTuple tup
        in MkBoltState $ toDyn $ PS.singleton key i

    , downstreamSerializer = \(MkPayloadTuple d) -> 
        show $ (forceDyn d :: [(String, Int)])

    , stateDeserializer = \str ->
        let pq = PS.fromList $ map (\(k,v) -> k PS.:-> v) $ (read str :: [(String, Int)])
        in MkBoltState $ toDyn $ pq

    , stateSerializer = \(MkBoltState psDyn) -> 
        show $ map (\(k PS.:-> v) -> (k,v)) $ PS.toList $ (forceDyn psDyn :: PS.PSQ String Int)
    }

outputAmt :: Int
outputAmt = 2

mergeSortBolt:: Bolt
mergeSortBolt = Bolt
    { boltName = "merge_sort"

    , boltParallelism = 1

    , upstreamDeserializer = \str ->
        MkPayloadTuple $ toDyn (read str :: [(String, Int)])

    , transformTupleFn = \_ (MkBoltState stateDyn) -> 
        let state = forceDyn $ stateDyn :: PS.PSQ String Int
        in MkPayloadTuple $ toDyn $ sortBy (flip $ comparing $ snd) $
              map (\(k PS.:-> v) -> (k,v)) (PS.toList state)

    , emptyState = 
        MkBoltState $ toDyn (PS.empty :: PS.PSQ String Int)

    , mergeFn = \(MkBoltState ps1Dyn) (MkBoltState ps2Dyn) ->
        let ps1 = forceDyn $ ps1Dyn :: PS.PSQ String Int
            ps2 = forceDyn $ ps2Dyn :: PS.PSQ String Int
        in MkBoltState $ toDyn $ mergePSTopN outputAmt ps1 ps2

    , tupleToStateConverter = \(MkPayloadTuple tup) ->
        let ls = map (\(k,v) -> k PS.:-> v) (forceDyn tup :: [(String, Int)])
        in MkBoltState $ toDyn $ PS.fromList ls

    , downstreamSerializer = \(MkPayloadTuple d) -> 
        show $ (forceDyn d :: [(String, Int)])

    , stateDeserializer = \str ->
        let pq = PS.fromList $ map (\(k,v) -> k PS.:-> v) $ (read str :: [(String, Int)])
        in MkBoltState $ toDyn $ pq

    , stateSerializer = \(MkBoltState psDyn) -> 
        show $ map (\(k PS.:-> v) -> (k,v)) $ PS.toList $ (forceDyn psDyn :: PS.PSQ String Int)
    }


printSorted :: Int -> Consumer PayloadTuple IO ()
printSorted cnt = do
  (MkPayloadTuple x) <- await
  lift $ when (cnt `mod` 2000 == 0) $
          print (forceDyn x :: [(String, Int)]) 
  printSorted (cnt + 1)

outputSink :: Sink
outputSink = Sink
    { sinkName = "sink"
    , sinkParallelism = 1
    , outputConsumer = printSorted 0
    --, outputFn = \_ -> return ()
    , sinkDeserializer = \str -> 
        MkPayloadTuple $ toDyn (read str:: [(String, Int)])
    }


wordCountTopology :: HardcodedTopology
wordCountTopology = HardcodedTopology
  {
      processorNodeMap = mkProcessorMap
      [ SpoutNode wordsSpout
      , BoltNode countBolt
      , BoltNode topNBolt
      , BoltNode mergeSortBolt
      , SinkNode outputSink
      ]
      ,
      downstreamMap = Map.fromList
      [ ("words", [("count", \(MkPayloadTuple dyn) -> 
                      hash $ fst $ (forceDyn dyn :: (String, TypeableIntSum)))])
      , ("count", [("topn", \(MkPayloadTuple dyn) ->
                      hash $ fst $ (forceDyn dyn :: (String, TypeableIntSum)))])
      , ("topn", [("merge_sort", const 0)]) 
      , ("merge_sort", [("sink", const 0)])
      ]
      ,
      addresses = Map.fromList
      [ (("sink", 0), (localServer, "10000"))
      , (("count", 0), (localServer, "10001"))
      , (("count", 1), (localServer, "10004"))
      , (("topn", 0), (localServer, "10002"))
      , (("topn", 1), (localServer, "10005"))
      , (("merge_sort", 0), (localServer, "10003"))
      ]
  }
