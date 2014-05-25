{-# LANGUAGE OverloadedStrings #-}

module Hailstorm.Processor.Runner
( runSpoutFromProducer
, runDownstream
, runNegotiator
) where

import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
import Data.List.Split
import Data.Maybe
import Data.IORef
import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Topology
import Hailstorm.ZKCluster
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map as Map
import qualified Data.Foldable as Foldable
import qualified Database.Zookeeper as ZK

data ConsumerType = BoltConsumer | SinkConsumer

debugSetMasterState :: ZK.Zookeeper -> MasterState -> IO (Either ZK.ZKError ZK.Stat)
debugSetMasterState zk ms = do
    r <- setMasterState zk ms
    putStrLn $ "Master state set to " ++ show ms
    return r

pauseUntilGreen :: MVar MasterState -> IO ()
pauseUntilGreen stateMVar = do
    ms <- readMVar stateMVar
    case ms of 
        GreenLight _ -> return ()
        _ -> threadDelay (1000 * 1000) >> pauseUntilGreen stateMVar

type Host = String
type Port = String
type BoltState k v = Map.Map k v

spoutStatePipe :: ZK.Zookeeper -> ProcessorId -> MVar MasterState -> Pipe (Payload k v) (Payload k v) IO ()
spoutStatePipe zk sid stateMVar = forever $ do
    ms <- lift $ readMVar stateMVar
    case ms of 
        GreenLight _ -> passOn
        SpoutPause ->  do
            _ <- lift $ forceEitherIO UnknownWorkerException (setProcessorState zk sid (SpoutPaused "fun" 0))
            lift $ pauseUntilGreen stateMVar
            _ <- lift $ forceEitherIO UnknownWorkerException (setProcessorState zk sid SpoutRunning)
            return ()
        _ -> do
            lift $ putStrLn $ "Spout waiting green light... master state=" ++ show ms
            lift $ threadDelay $ 1000 * 1000 * 10
    where passOn = await >>= yield

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

        monitorMasterState zk $ \et -> case et of
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
    registerProcessor opts did SinkRunning $ const $ serve HostAny port $ \(s, _) -> processSocket s
    throw $ ZookeeperConnectionError
        "Sink zookeeper registration terminated unexpectedly"

killFromRef :: IORef (Maybe ThreadId) -> IO ()
killFromRef ioRef = do
    mt <- readIORef ioRef
    Foldable.forM_ mt killThread

waitUntilSnapshotsComplete :: (Topology t) => ZK.Zookeeper -> t -> IO ()
waitUntilSnapshotsComplete _ _ = return ()

negotiateSnapshot :: (Topology t) => ZK.Zookeeper -> t -> IO Clock
negotiateSnapshot zk t = do
    _ <- forceEitherIO UnknownWorkerException (debugSetMasterState zk SpoutPause)
    offsetsAndPartitions <- untilSpoutsPaused
    return $ Clock (Map.fromList offsetsAndPartitions)

    where untilSpoutsPaused = do
            stateMap <- forceEitherIO UnknownWorkerException (getAllProcessorStates zk)
            let spoutStates = map (\k -> fromJust $ Map.lookup k stateMap) (spoutIds t)
            let spoutsPaused = [(p,o) | (SpoutPaused p o) <- spoutStates]
            if length spoutsPaused == length spoutStates then return spoutsPaused
                else untilSpoutsPaused

runNegotiator :: Topology t => ZKOptions -> t -> IO ()
runNegotiator zkOpts topology = do
    fullChildrenThreadId <- newIORef (Nothing :: Maybe ThreadId)
    registerProcessor zkOpts ("negotiator", 0) UnspecifiedState $ \zk ->
        forceEitherIO
            (DuplicateNegotiatorError
                "Could not set state, probable duplicate process")
            (createMasterState zk Initialization) >> watchLoop zk fullChildrenThreadId
    throw $ ZookeeperConnectionError
        "Negotiator zookeeper registration terminated unexpectedly"

  where
    fullThread zk = forever $ do
        waitUntilSnapshotsComplete zk topology
        threadDelay $ 1000 * 1000 * 5
        nextSnapshotClock <- negotiateSnapshot zk topology 
        _ <- forceEitherIO UnknownWorkerException (debugSetMasterState zk (GreenLight nextSnapshotClock))
        return ()
        
    watchLoop zk fullThreadId = childrenWatchLoop zk "/living_processors" $ \children -> do
      killFromRef fullThreadId       

      putStrLn $ "Children changed to " ++ show children
      let expectedRegistrations = numProcessors topology + 1

      if length children < expectedRegistrations
        then do
            putStrLn "Not enough children"
            _ <- forceEitherIO UnexpectedZookeeperError (debugSetMasterState zk Unavailable)
            return ()
        else do
            tid <- forkOS $ fullThread zk
            writeIORef fullThreadId $ Just tid

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

