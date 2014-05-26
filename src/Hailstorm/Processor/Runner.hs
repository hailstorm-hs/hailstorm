{-# LANGUAGE OverloadedStrings #-}

module Hailstorm.Processor.Runner
( runSpout
, runDownstream
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
import Data.List.Split
import Data.Maybe
import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.InputSource
import Hailstorm.MasterState
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Topology
import Hailstorm.ZKCluster
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK

type Host = String
type Port = String
type BoltState k v = Map.Map k v

-- | Start processing with a spout
runSpout :: (Topology t, InputSource s)
         => ZKOptions
         -> ProcessorName
         -> Partition
         -> t
         -> s
         -> UserFormula k v
         -> IO ()

runSpout zkOpts pName partition topology inputSource uf = do
    index <- partitionIndex inputSource partition
    let spoutId = (pName, index)

    registerProcessor zkOpts spoutId SpoutRunning $ \zk -> do
        stateMVar <- newEmptyMVar
        _ <- forkOS $ pipeThread zk spoutId stateMVar

        watchMasterState zk $ \et -> case et of
            Left e -> throw $ wrapInHSError e UnexpectedZookeeperError
            Right ms -> do
                putStrLn $ "Spout: detected master state change to " ++ show ms
                tryTakeMVar stateMVar >> putMVar stateMVar ms -- Overwrite
    throw $ ZookeeperConnectionError
        "Spout zookeeper registration terminated unexpectedly"
  where
    pipeThread zk spoutId stateMVar =
      let downstream = downstreamConsumer (fst spoutId) topology uf
          producer = partitionToPayloadProducer uf $ partitionProducer inputSource partition 0
      in runEffect $
        producer >-> spoutStatePipe zk spoutId stateMVar >-> downstream


runDownstream :: (Ord k, Monoid v, Topology t)
              => ZKOptions
              -> ProcessorId
              -> t
              -> UserFormula k v
              -> IO ()
runDownstream opts did@(dname, _) topology uformula = do
    let (_, port) = addressFor topology did
        ctype = processorType (fromJust $ Map.lookup dname
            (processors topology))
        producer = socketProducer uformula
        consumer =
            case ctype of
                Bolt -> boltPipe uformula Map.empty >->
                    downstreamConsumer dname topology uformula
                Sink -> sinkConsumer uformula
                _ -> error "Non-consumer processor provided"
        processSocket s = runEffect $ producer s >-> consumer
    registerProcessor opts did SinkRunning $ const $ serve HostAny port $
      \(s, _) -> processSocket s
    throw $ ZookeeperConnectionError
        "Sink zookeeper registration terminated unexpectedly"

spoutStatePipe :: ZK.Zookeeper
               -> ProcessorId
               -> MVar MasterState
               -> Pipe (Payload k v) (Payload k v) IO ()
spoutStatePipe zk spoutId stateMVar = forever $ do
    ms <- lift $ readMVar stateMVar
    case ms of
        ValveOpened _ -> passOn
        ValveClosed ->  do
            void <$> lift $ forceEitherIO UnknownWorkerException
                (setProcessorState zk spoutId $ SpoutPaused "fun" 0)
            lift $ pauseUntilValveOpened stateMVar
            void <$> lift $ forceEitherIO UnknownWorkerException
                (setProcessorState zk spoutId SpoutRunning)
        _ -> do
            lift $ putStrLn $
                "Spout waiting for open valve (state: " ++ show ms ++ ")"
            lift $ threadDelay $ 1000 * 1000 * 10
  where passOn = await >>= yield

pauseUntilValveOpened :: MVar MasterState -> IO ()
pauseUntilValveOpened stateMVar = do
    ms <- readMVar stateMVar
    case ms of
        ValveOpened _ -> return ()
        _ -> threadDelay (1000 * 1000) >> pauseUntilValveOpened stateMVar

-- | Returns a Producer that receives a stream of payloads through a given
-- socket and deserializes them.
socketProducer :: UserFormula k v
               -> Socket
               -> Producer (Payload k v) IO ()
socketProducer uformula s = do
    h <- lift $ socketToHandle s ReadMode
    lift $ hSetBuffering h LineBuffering
    emitNextPayload h
  where
    emitNextPayload h = do
        t <- lift $ hGetLine h
        let [sTuple, sClock] = splitOn "\1" t
            tuple = deserialize uformula sTuple
            clock = read sClock :: Clock
        yield (Payload tuple clock)
        emitNextPayload h

-- | Builds a Pipe that receives a payload emitted from a handle and
-- performs the monoidal append operation associated with the given processor.
boltPipe :: (Ord k, Monoid v)
         => UserFormula k v
         -> BoltState k v
         -> Pipe (Payload k v) (Payload k v) IO ()
boltPipe uformula state = do
    payload <- await
    let (key, val) = payloadTuple payload
        oldval = Map.findWithDefault mempty key state
        newval = oldval `mappend` val
    yield $ Payload (key, newval) (payloadClock payload)
    boltPipe uformula $ Map.union (Map.singleton key newval) state

-- | Builds a Consumer that receives a payload emitted from a handle and
-- performs the sink operation defined in the given user formula.
sinkConsumer :: UserFormula k v -> Consumer (Payload k v) IO ()
sinkConsumer uformula = forever $ do
    payload <- await
    lift $ outputFn uformula (payloadTuple payload)

-- | @poolConnect address handleMap@ will return a handle for communication
-- with a processor, using an existing handle if one exists in
-- @handleMap@, creating a new connection to the host otherwise.
poolConnect :: (Host, Port) -> Map.Map (Host, Port) Handle -> IO Handle
poolConnect (host, port) handleMap = case Map.lookup (host, port) handleMap of
    Just h -> return h
    Nothing -> connect host port $ \(s, _) -> socketToHandle s WriteMode

-- | Produces a single Consumer comprised of all stream consumer layers of
-- the topology (bolts and sinks) that subscribe to a emitting processor's
-- stream. Payloads received by the consumer are sent to the next layer in
-- the topology.
downstreamConsumer :: Topology t
                   => ProcessorName
                   -> t
                   -> UserFormula k v
                   -> Consumer (Payload k v) IO ()
downstreamConsumer processorName topology uformula = emitToNextLayer Map.empty
  where
    emitToNextLayer connPool = do
        payload <- await
        let sendAddresses = downstreamAddresses topology processorName payload
            getHandle addressTuple = lift $ poolConnect addressTuple connPool
            emitToHandle h = (lift . hPutStrLn h) $
                serialize uformula (payloadTuple payload) ++ "\1" ++
                    show (payloadClock payload)
        newHandles <- mapM getHandle sendAddresses
        mapM_ emitToHandle newHandles
        let newPool = Map.fromList $ zip sendAddresses newHandles
        emitToNextLayer $ Map.union newPool connPool
