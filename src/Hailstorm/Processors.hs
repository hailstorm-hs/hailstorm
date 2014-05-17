{-# LANGUAGE OverloadedStrings #-}

module Hailstorm.Processors where

import Control.Monad
import Data.ByteString.Char8 ()
import Data.List.Split
import Data.Maybe
import Hailstorm.UserFormula
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.ByteString as B
import qualified Data.Map as Map

data DataPartitionOffset = DataPartitionOffset B.ByteString String Integer
    deriving (Eq, Show, Read)

data HailstormPayload k v = HailstormPayload {
    payloadTuple :: (k,v),
    payloadClock :: HailstormClock
} deriving (Eq, Show, Read)

newtype HailstormClock = HailstormClock (Map.Map String Integer)
    deriving (Eq, Show, Read)

class Topology t where 
    downstreamFor :: t -> String -> HailstormPayload k v -> (String, String)

data HardcodedTopology = HardcodedTopology (Map.Map String [(String, String)])
    deriving (Eq, Show, Read)

instance Topology HardcodedTopology where
    downstreamFor (HardcodedTopology tmap) processorId _ = 
        head downstreams
        where downstreams = fromJust $ Map.lookup processorId tmap
    
tupleSpoutProducer :: (Show k, Show v, Monad m) => UserFormula k v -> Producer DataPartitionOffset m () -> Producer (HailstormPayload k v) m ()
tupleSpoutProducer uf producer = for producer (\x -> case x of (DataPartitionOffset bs p o) ->  yield (HailstormPayload (convertFn uf bs) (HailstormClock $ Map.singleton p o)))

handleLineProducer :: Handle -> String -> Integer -> Producer DataPartitionOffset IO ()
handleLineProducer h partitionName offset = do
    eof <- lift $ hIsEOF h
    when eof $ lift (hSeek h AbsoluteSeek 0)
    l <- lift $ B.hGetLine h
    yield $ DataPartitionOffset l partitionName offset
    handleLineProducer h partitionName (offset + 1)


fileLineProducer :: FilePath -> Producer DataPartitionOffset IO ()
fileLineProducer fp = do
    h <- lift $ openFile fp ReadMode
    handleLineProducer h (show fp) 0

poolConnect :: (String, String) -> Map.Map (String, String) Handle -> IO Handle
poolConnect (host, port) m = case Map.lookup (host, port) m of
    Just so -> return so
    Nothing -> connect host port (\(s, _) -> socketToHandle s WriteMode)

downstreamConsumer :: (Show k, Show v, Topology t) => UserFormula k v -> String -> t -> Consumer (HailstormPayload k v) IO ()
downstreamConsumer uf processorId topology = dcInternal Map.empty
    where dcInternal connectionPool = do
            payload <- await
            let (host, port) = downstreamFor topology processorId payload
            h <- lift $ poolConnect (host, port) connectionPool
            lift $ hPutStrLn h (serialize uf (payloadTuple payload) ++ "\1" ++ show (payloadClock payload))
            dcInternal $ Map.insert (host, port) h connectionPool

socketProducer :: (Read k, Read v) => UserFormula k v -> Socket -> Producer (HailstormPayload k v) IO ()
socketProducer uf s = lift (socketToHandle s ReadMode) >>= producerInternal
    where producerInternal h = do
            t <- lift $ hGetLine h
            let [sTuple, sClock] = splitOn "\1" t
                tuple = deserialize uf sTuple
                clock :: HailstormClock 
                clock = read sClock

            yield (HailstormPayload tuple clock)
            producerInternal h
