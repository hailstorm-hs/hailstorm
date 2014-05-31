{-# LANGUAGE ScopedTypeVariables #-}

module Hailstorm.Processor.Downstream
( BoltState
, runDownstream
) where

import Control.Concurrent hiding (yield)
import Control.Applicative
import Control.Exception
import Control.Monad
import Control.Monad.STM
import Data.ByteString.Char8 ()
import Data.IORef
import Data.Maybe
import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Processor.Pool
import Hailstorm.SnapshotStore
import Hailstorm.Topology
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.MasterState
import Hailstorm.ZKCluster.ProcessorState
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK
import qualified Pipes.Concurrent as PC
import qualified Network.Socket as NS

type BoltState k v = Map.Map k v

runDownstream :: (Ord k, Monoid v, Topology t, Show k, Show v, SnapshotStore s)
              => ZKOptions
              -> ProcessorId
              -> t
              -> UserFormula k v
              -> s
              -> IO ()
runDownstream opts dId@(dName, _) topology uformula snapshotStore = do
    (stateMap', savedClk) <- restoreSnapshot snapshotStore dId $
        deserializeState uformula

    let (_, port) = addressFor topology dId
        ctype = processorType $ fromJust $ Map.lookup dName $
            processors topology
        producer = socketProducer uformula
        throwNoDownstreamError = throw $ InvalidTopologyError $
            dName ++ " is not a downstream processor"
        consumer zk mStateMVar =
            case ctype of
                Bolt ->
                    let savedState | (Just stateMap) <- stateMap' = stateMap
                                   | otherwise = Map.empty
                    in boltPipe dId zk mStateMVar savedState savedClk
                        snapshotStore >->
                        downstreamPoolConsumer dName topology uformula
                Sink -> sinkConsumer uformula
                _ -> throwNoDownstreamError
        startState = case ctype of
                         Sink -> SinkRunning
                         Bolt -> BoltLoaded savedClk
                         _ -> throwNoDownstreamError
        processSocket s pcOutput = runEffect $
            producer s >-> PC.toOutput pcOutput

    groundhogDay (runDownstream opts dId topology uformula snapshotStore) $ do
        (pcOutput, pcInput, seal) <- PC.spawn' PC.Unbounded
        serverRef <- newIORef (Nothing :: Maybe ThreadId)
        
        finally (registerProcessor opts dId startState $ \zk -> do
                    serveId <- forkOS $ serveForkOS HostAny port $ \(s, _) -> processSocket s pcOutput
                    writeIORef serverRef (Just serveId)

                    injectMasterState zk $ \mStateMVar -> 
                        runEffect $ PC.fromInput pcInput >-> consumer zk mStateMVar
                ) (atomically seal >> killRef serverRef)

    throw $ ZookeeperConnectionError $ "Unable to register downstream " ++ dName


killRef :: IORef (Maybe ThreadId) -> IO ()
killRef iref = do
    mtid <- readIORef iref
    case mtid of 
        Just tid -> killThread tid
        Nothing -> return ()


serveForkOS :: HostPreference
            -> ServiceName
            -> ((Socket, SockAddr) -> IO ()) -> IO r
serveForkOS hp port k = do
    listen hp port $ \(lsock,_) -> do
      forever $ acceptForkOS lsock k

acceptForkOS
  :: NS.Socket 
  -> ((NS.Socket, NS.SockAddr) -> IO ())
  -> IO ThreadId
acceptForkOS lsock f = do
    client@(csock,_) <- NS.accept lsock
    forkOS $ finally (f client) (NS.sClose csock)

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
        yield $ deserializePayload t uformula
        emitNextPayload h

-- | Builds a Pipe that receives a payload emitted from a handle and
-- performs the monoidal append operation associated with the given processor.
boltPipe :: (Ord k, Monoid v, Show k, Show v, SnapshotStore s)
         => ProcessorId
         -> ZK.Zookeeper
         -> MVar MasterState
         -> BoltState k v
         -> Clock
         -> s
         -> Pipe (Payload k v) (Payload k v) IO ()
boltPipe bId@(bName, _) zk mStateMVar state clk snapshotStore =
    pipeLoop state Map.empty False clk
  where
    pipeLoop preSnapState postSnapState started lastClock = do
        payload <- await

        -- If this is the first payload received, mark as Saved instead.
        unless started $
            lift $ forceSetProcessorState zk bId $ BoltSaved lastClock

        let (key, val) = payloadTuple payload
            (partition, offset) = payloadPosition payload
            lwmMap = payloadLowWaterMarkMap payload
            newLWM = buildLWM lwmMap
            newLWMMap = Map.union (Map.singleton bName newLWM) lwmMap

        let passOn stateA stateB desiredSnapClock = do
                let valA = Map.findWithDefault mempty key stateA
                    valB = Map.findWithDefault mempty key stateB

                yield Payload { payloadTuple = (key, valA `mappend` valB)
                            , payloadPosition = (partition, offset)
                            , payloadLowWaterMarkMap = newLWMMap
                            }

                if canSnapshot desiredSnapClock newLWM lastClock
                    then do
                        void <$> lift $ saveState bId zk stateA
                            (fromJust desiredSnapClock) snapshotStore
                        pipeLoop (stateA `mergeStates` stateB) Map.empty
                            True $ fromJust desiredSnapClock
                    else pipeLoop stateA stateB True lastClock

        -- Determine next snapshot clock, if available.
        mState <- lift $ readMVar mStateMVar
        case getNextSnapshotClock mState of
            Just desiredClock@(Clock clockMap) -> do
                let desiredOffset = clockMap Map.! partition
                if offset > desiredOffset
                    then let b' = mergeWithTuple postSnapState (key, val)
                        in passOn preSnapState b' (Just desiredClock)
                    else let a' = mergeWithTuple preSnapState (key, val)
                        in passOn a' postSnapState (Just desiredClock)
            Nothing ->
                let mergedState = preSnapState `mergeStates` postSnapState
                in passOn (mergeWithTuple mergedState (key, val)) Map.empty Nothing

    mergeWithTuple st (key, val) = Map.unionWith mappend st $
        Map.singleton key val

    mergeLWM = Map.unionWith min

    mergeStates = Map.unionWith mappend

    buildLWM lwmMap = Clock $ foldr (mergeLWM . extractClockMap) Map.empty $
        Map.elems lwmMap

    canSnapshot (Just desiredSnap) lwm lastSnap = desiredSnap /= lastSnap &&
        lwm `clockGt` desiredSnap
    canSnapshot Nothing _ _ = False

saveState :: (Show k, Show v, SnapshotStore s)
          => ProcessorId
          -> ZK.Zookeeper
          -> BoltState k v
          -> Clock
          -> s
          -> IO ThreadId
saveState pId zk bState clk snapshotStore = forkOS $ do
    saveSnapshot snapshotStore pId bState clk
    forceSetProcessorState zk pId (BoltSaved clk)

-- | Builds a Consumer that receives a payload emitted from a handle and
-- performs the sink operation defined in the given user formula.
sinkConsumer :: UserFormula k v -> Consumer (Payload k v) IO ()
sinkConsumer uformula = forever $ do
    payload <- await
    lift $ outputFn uformula $ payloadTuple payload
