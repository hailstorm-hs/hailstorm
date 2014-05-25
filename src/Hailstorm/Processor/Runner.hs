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
    putStrLn $ "Master state set to " ++ (show ms)
    return r

pauseUntilGreen :: MVar MasterState -> IO ()
pauseUntilGreen stateMVar = do
    ms <- readMVar stateMVar
    case ms of 
        GreenLight _ -> return ()
        _ -> threadDelay (1000 * 1000 * 1) >> pauseUntilGreen stateMVar

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

runSpoutFromProducer :: (Show k, Show v, Topology t)
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
            (Left e) -> throw $ wrapInHSError e UnexpectedZookeeperError
            (Right ms) -> do 
                putStrLn $ "Spout: detected master state change to " ++ show ms
                tryTakeMVar stateMVar >> putMVar stateMVar ms -- Overwrite

    throw $ ZookeeperConnectionError
        "Spout zookeeper registration terminated unexpectedly"

    
    where pipeThread zk stateMVar = let downstream = downstreamConsumer sname topology uf in 
                       runEffect $ producer >-> spoutStatePipe zk sid stateMVar >-> downstream
            

        

runDownstream :: (Show k, Show v, Read k, Read v, Topology t)
              => ZKOptions
              -> ProcessorId
              -> t
              -> UserFormula k v
              -> IO ()
runDownstream opts did@(dname, _) topology uformula = do
    let (_, port) = addressFor topology did
        ctype = consumerType (fromJust $ Map.lookup dname (processors topology))
        accepted socket = let sp = socketProducer uformula socket
                              fc = formulaConsumer uformula ctype
                          in runEffect $ sp >-> fc
    registerProcessor opts did SinkRunning $ const $ serve HostAny port $ \(s, _) -> accepted s

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

runNegotiator :: (Topology t) => ZKOptions -> t -> IO ()
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
        putStrLn "Master state set to green light"
        
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


-- | Builds a payload consumer using the provided UserFormula and based
-- on the operation
formulaConsumer :: UserFormula k v
                -> ConsumerType
                -> Consumer (Payload k v) IO ()
formulaConsumer uf tconsumer = forever $ do
    payload <- await
    case tconsumer of
      BoltConsumer -> lift $ outputFn uf (payloadTuple payload)
      SinkConsumer -> lift $ outputFn uf (payloadTuple payload)

socketProducer :: (Read k, Read v)
               => UserFormula k v
               -> Socket
               -> Producer (Payload k v) IO ()
socketProducer uf s = do
    h <- lift (socketToHandle s ReadMode)
    lift $ hSetBuffering h LineBuffering
    producerInternal h
  where
    producerInternal h = do
        t <- lift $ hGetLine h
        let [sTuple, sClock] = splitOn "\1" t
            tuple = deserialize uf sTuple
            clock = read sClock :: Clock
        yield (Payload tuple clock)
        producerInternal h

poolConnect :: (String, String) -> Map.Map (String, String) Handle -> IO Handle
poolConnect (host, port) m = case Map.lookup (host, port) m of
    Just so -> return so
    Nothing -> connect host port (\(s, _) -> socketToHandle s WriteMode)

downstreamConsumer :: (Show k, Show v, Topology t)
                   => ProcessorName
                   -> t
                   -> UserFormula k v
                   -> Consumer (Payload k v) IO ()
downstreamConsumer processorName topology uf = dcInternal Map.empty
    where dcInternal connectionPool = do
            payload <- await
            let sendAddresses = downstreamAddresses topology processorName payload
                addressToHandle (host, port) = do
                    h <- lift $ poolConnect (host, port) connectionPool
                    lift $ hPutStrLn h (serialize uf (payloadTuple payload) ++
                      "\1" ++ show (payloadClock payload))
                    return ((host, port), h)
            newHandles <- mapM addressToHandle sendAddresses
            dcInternal $ Map.union (Map.fromList newHandles) connectionPool

consumerType :: Processor -> ConsumerType
consumerType Bolt{} = BoltConsumer
consumerType Sink{} = SinkConsumer
consumerType _ = error "Given processor is not a consumer"
