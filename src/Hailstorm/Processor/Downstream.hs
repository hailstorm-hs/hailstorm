{-# LANGUAGE ScopedTypeVariables #-}

module Hailstorm.Processor.Downstream
( runDownstream
) where

import Control.Concurrent hiding (yield)
import Control.Applicative
import Control.Exception
import Control.Monad
import Control.Monad.STM
import Data.ByteString.Char8 ()
import Data.Maybe
import Data.IORef
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Processor.Pool
import Hailstorm.SnapshotStore
import Hailstorm.InputSource
import Hailstorm.Topology
import Hailstorm.TransactionTypes
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.MasterState
import Hailstorm.ZKCluster.ProcessorState
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map.Strict as Map
import qualified Database.Zookeeper as ZK
import qualified Pipes.Concurrent as PC
import qualified Pipes.Prelude as P
import qualified Network.Socket as NS
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Processor.Downstream"

runDownstream :: (Topology t, SnapshotStore s, InputSource i)
              => ZKOptions
              -> ProcessorId
              -> t
              -> i
              -> s
              -> IO ()
runDownstream opts dId@(dName, dInst) topology inputSource snapshotStore = do
    let pr = lookupProcessorWithFailure dName topology

    -- Restore snapshot, if available.
    (st', savedClk) <-
        case pr of
            (BoltNode b) -> restoreSnapshot snapshotStore dId $
                stateDeserializer b
            _ -> return (Nothing, Clock Map.empty)

    let (_, port) = addressFor topology dId
        producer = socketProducer pr
        throwNoDownstreamError = throw $ InvalidTopologyError $
            dName ++ " is not a downstream processor"
        consumer zk mStateMVar =
            case pr of
                BoltNode b ->
                    let savedState | (Just st) <- st' = st
                                   | otherwise = emptyState b
                    in boltPipe b dInst zk mStateMVar savedState savedClk
                        snapshotStore >->
                        downstreamPoolConsumer dName topology

                SinkNode k -> P.map payloadTuple >-> outputConsumer k
                _ -> throwNoDownstreamError
        processSocket s pcOutput = runEffect $
            producer s >-> PC.toOutput pcOutput

    startState <- case pr of
                      SinkNode _ -> return SinkRunning
                      BoltNode _ -> if not $ Map.null $ extractClockMap savedClk
                                        then return $ BoltLoaded savedClk
                                        else do
                                            startClk <- startClock inputSource
                                            infoM $ "Bolt beginning at start clock " ++ show startClk
                                            return $ BoltLoaded startClk
                      _ -> throwNoDownstreamError

    groundhogDay dId (runDownstream opts dId topology inputSource snapshotStore) $ do
        (pcOutput, pcInput, seal) <- PC.spawn' PC.Single
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
serveForkOS hp port k =
    listen hp port $ \(lsock,_) -> forever $ acceptForkOS lsock k

acceptForkOS
  :: NS.Socket
  -> ((NS.Socket, NS.SockAddr) -> IO ())
  -> IO ThreadId
acceptForkOS lsock f = do
    client@(csock,_) <- NS.accept lsock
    forkOS $ finally (f client) (NS.sClose csock)

-- | Returns a Producer that receives a stream of payloads through a given
-- socket and deserializes them.
socketProducer :: ProcessorNode
               -> Socket
               -> Producer Payload IO ()
socketProducer p s = do
    h <- lift $ socketToHandle s ReadWriteMode
    lift $ hSetBuffering h LineBuffering
    emitNextPayload h
  where
    emitNextPayload h = do
        t <- lift $ hGetLine h
        yield $ deserializePayload t $ deserializer p
        lift $ hPutStrLn h "OK"
        emitNextPayload h

-- | Builds a Pipe that receives a payload emitted from a handle and
-- performs the monoidal append operation associated with the given processor.
boltPipe :: (SnapshotStore s)
         => Bolt
         -> ProcessorInstance
         -> ZK.Zookeeper
         -> MVar MasterState
         -> BoltState
         -> Clock
         -> s
         -> Pipe Payload Payload IO ()
boltPipe blt instNum zk mStateMVar state clk snapshotStore =
    pipeLoop state (emptyState blt) False clk
  where
    bName = boltName blt
    bId = (bName, instNum)
    pipeLoop preSnapState postSnapState started lastClock = do
        payload <- await

        -- If this is the first payload received, mark as Saved instead.
        unless started $
            lift $ forceSetProcessorState zk bId $ BoltSaved lastClock

        let tup = payloadTuple payload
            (partition, offset) = payloadPosition payload
            lwmMap = payloadLowWaterMarkMap payload
            newLWM = buildLWM lwmMap
            newLWMMap = Map.union (Map.singleton bName newLWM) lwmMap

        let passOn stateA stateB desiredSnapClock = do
                let fullState = mergeFn blt stateA stateB
                yield Payload { payloadTuple = transformTupleFn blt tup fullState
                              , payloadPosition = (partition, offset)
                              , payloadLowWaterMarkMap = newLWMMap
                              }

                if canSnapshot desiredSnapClock newLWM lastClock
                    then do
                        lift $ infoM $
                            "Perming snapshot at " ++ show desiredSnapClock ++
                                " for bolt " ++ show bId
                        let snappy = fromMaybe
                                (throw $ BadStateError
                                    "Expected desired snapshot clock to be non-empty")
                                desiredSnapClock

                        void <$> lift $ saveState blt instNum zk stateA
                             snappy snapshotStore
                        pipeLoop (stateA `mergeStates` stateB) (emptyState blt)
                            True snappy
                    else pipeLoop stateA stateB True lastClock

        -- Determine next snapshot clock, if available.
        mState <- lift $ readMVar mStateMVar
        case getNextSnapshotClock mState of
            Just desiredClock@(Clock clockMap) -> do
                let desiredOffset = clockMap Map.! partition
                if offset > desiredOffset
                    then let b' = mergeWithTuple postSnapState tup
                         in passOn preSnapState b' (Just desiredClock)
                    else let a' = mergeWithTuple preSnapState tup
                         in passOn a' postSnapState (Just desiredClock)
            Nothing ->
                let mergedState = preSnapState `mergeStates` postSnapState
                in passOn (mergeWithTuple mergedState tup) (emptyState blt) Nothing

    mergeWithTuple st tup = mergeFn blt (tupleToStateConverter blt tup) st

    mergeLWM = Map.unionWith min

    mergeStates = mergeFn blt

    buildLWM lwmMap = Clock $ foldr (mergeLWM . extractClockMap) Map.empty $
        Map.elems lwmMap

    canSnapshot (Just desiredSnap) lwm lastSnap = desiredSnap /= lastSnap &&
        lwm `clockGt` desiredSnap
    canSnapshot Nothing _ _ = False

saveState :: (SnapshotStore s)
          => Bolt
          -> ProcessorInstance
          -> ZK.Zookeeper
          -> BoltState
          -> Clock
          -> s
          -> IO ThreadId
saveState blt instNum zk bState clk snapshotStore = forkOS $ do
    let pId = (boltName blt, instNum)
    saveSnapshot snapshotStore pId bState (stateSerializer blt) clk
    forceSetProcessorState zk pId (BoltSaved clk)
