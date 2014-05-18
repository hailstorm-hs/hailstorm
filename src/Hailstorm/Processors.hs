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
import qualified Hailstorm.Zookeeper as HSZK
import qualified Data.ByteString as B
import qualified Data.Map as Map

data DataPartitionOffset = DataPartitionOffset B.ByteString String Integer
    deriving (Eq, Show, Read)

data HailstormPayload k v = HailstormPayload {
    payloadTuple :: (k,v)
  , payloadClock :: HailstormClock
} deriving (Eq, Show, Read)

newtype HailstormClock = HailstormClock (Map.Map String Integer)
    deriving (Eq, Show, Read)

class Topology t where 
    downstreamAddresses :: t -> String -> HailstormPayload k v -> [(String, String)]
    addressFor :: t -> (String, Int) -> (String, String)

data HailstormProcessor = Spout {name :: String, parallelism :: Int, downstreams :: [String]}
                        | Sink {name :: String, parallelism :: Int}
    deriving (Eq, Show, Read)

data HardcodedTopology = HardcodedTopology {
    processorMap :: Map.Map String HailstormProcessor
  , addresses :: Map.Map (String, Int) (String, String)
} deriving (Eq, Show, Read)

mkProcessorMap :: [HailstormProcessor] -> Map.Map String HailstormProcessor
mkProcessorMap ps = Map.fromList $ map (\x -> (name x, x)) ps

instance Topology HardcodedTopology where
    downstreamAddresses t processorName _ = 
        let upstream = fromJust $ Map.lookup processorName (processorMap t) in
        map findAddress (downstreams upstream)

        where findAddress downstreamName = 
               let downstream = fromJust $ Map.lookup downstreamName (processorMap t) in
               fromJust $ Map.lookup ((name downstream), (parallelism downstream) - 1) (addresses t)

    addressFor t (processorName, processorNumber) = fromJust $ Map.lookup (processorName, processorNumber) (addresses t)

    
dataToPayloadPipe :: (Show k, Show v, Monad m) => UserFormula k v -> Producer DataPartitionOffset m () -> Producer (HailstormPayload k v) m ()
dataToPayloadPipe uf producer = for producer (\x -> case x of (DataPartitionOffset bs p o) ->  yield (HailstormPayload (convertFn uf bs) (HailstormClock $ Map.singleton p o)))

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

downstreamConsumer :: (Show k, Show v, Topology t) => String -> t -> UserFormula k v -> Consumer (HailstormPayload k v) IO ()
downstreamConsumer processorId topology uf = dcInternal Map.empty
    where dcInternal connectionPool = do
            payload <- await
            let sendAddresses = downstreamAddresses topology processorId payload
            newHandles <- mapM (\(host, port) -> do
                                    h <- lift $ poolConnect (host, port) connectionPool
                                    lift $ hPutStrLn h (serialize uf (payloadTuple payload) ++ "\1" ++ show (payloadClock payload))
                                    return ((host, port), h)
                               ) sendAddresses
            dcInternal $ Map.union (Map.fromList newHandles) connectionPool

socketProducer :: (Read k, Read v) => UserFormula k v -> Socket -> Producer (HailstormPayload k v) IO ()
socketProducer uf s = do
    h <- lift (socketToHandle s ReadMode) 
    lift $ hSetBuffering h LineBuffering
    producerInternal h
    where producerInternal h = do
            t <- lift $ hGetLine h
            let [sTuple, sClock] = splitOn "\1" t
                tuple = deserialize uf sTuple
                clock :: HailstormClock 
                clock = read sClock

            yield (HailstormPayload tuple clock)
            producerInternal h


runSpoutFromProducer :: (Show k, Show v, Topology t) => String -> t -> UserFormula k v -> Producer (HailstormPayload k v) IO () -> IO ()
runSpoutFromProducer spoutId topology uf producer = 
    let downstream = downstreamConsumer spoutId topology uf in
    runEffect $ producer >-> downstream

formulaConsumer :: UserFormula k v -> Consumer (HailstormPayload k v) IO ()
formulaConsumer uf = forever $ do
   payload <- await 
   lift $ outputFn uf (payloadTuple payload)

runSink :: (Show k, Show v, Read k, Read v, Topology t) => HSZK.ZKOptions -> (String, Int) ->  t -> UserFormula k v -> IO ()
runSink zkOpts (processorName, offset) topology uf = do
    let (_, port) = addressFor topology (processorName, offset)
    HSZK.connectAndRegisterProcessor zkOpts processorName (\_ -> do
            putStrLn $ "Registered " ++ processorName ++ " in Zookeeper"
            serve HostAny port (\(s, _) -> accepted s)
        )
    where accepted socket = 
            let sp = socketProducer uf socket
                fc = formulaConsumer uf in
            runEffect $ sp >-> fc
