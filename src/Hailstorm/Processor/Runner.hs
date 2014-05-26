{-# LANGUAGE OverloadedStrings #-}

module Hailstorm.Processor.Runner
( runSpoutFromProducer
, runDownstream
) where

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
import Hailstorm.Negotiator
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Topology
import Hailstorm.ZKCluster
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map as Map

data ConsumerType = BoltConsumer | SinkConsumer

type Host = String
type Port = String
type BoltState k v = Map.Map k v

-- | Register a spout processor on Zookeeper backed by a Kafka partition*.
-- TODO*: Change source from a file to a Kafka partition.
runSpoutFromProducer :: Topology t
                     => ZKOptions
                     -> ProcessorId
                     -> t
                     -> UserFormula k v
                     -> Producer (Payload k v) IO ()
                     -> IO ()
runSpoutFromProducer zkOpts sid@(sname, _) topology uf producer = do
    registerProcessor zkOpts sid SpoutRunning $ \zk -> do
        stateMVar <- newEmptyMVar
        _ <- forkOS $ pipeThread zk stateMVar

        watchMasterState zk $ \et -> case et of
            Left e -> throw $ wrapInHSError e UnexpectedZookeeperError
            Right ms -> do
                putStrLn $ "Spout: detected master state change to " ++ show ms
                tryTakeMVar stateMVar >> putMVar stateMVar ms -- Overwrite
    throw $ ZookeeperConnectionError
        "Spout zookeeper registration terminated unexpectedly"
  where
    pipeThread zk stateMVar =
      let downstream = downstreamConsumer sname topology uf
      in runEffect $ producer >-> spoutStatePipe zk sid stateMVar >-> downstream

runDownstream :: (Ord k, Monoid v, Topology t)
              => ZKOptions
              -> ProcessorId
              -> t
              -> UserFormula k v
              -> IO ()
runDownstream opts did@(dname, _) topology uformula = do
    let (_, port) = addressFor topology did
        ctype = consumerType (fromJust $ Map.lookup dname (processors topology))
        producer = socketProducer uformula
        consumer =
            case ctype of
                BoltConsumer -> boltPipe uformula Map.empty >->
                    downstreamConsumer dname topology uformula
                SinkConsumer -> sinkConsumer uformula
        processSocket s = runEffect $ producer s >-> consumer
    registerProcessor opts did SinkRunning $ const $ serve HostAny port $
      \(s, _) -> processSocket s
    throw $ ZookeeperConnectionError
        "Sink zookeeper registration terminated unexpectedly"

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

consumerType :: Processor -> ConsumerType
consumerType Bolt{} = BoltConsumer
consumerType Sink{} = SinkConsumer
consumerType _ = error "Given processor is not a consumer"

