module Hailstorm.Processor.Downstream
( BoltState
, runDownstream
) where

import Control.Concurrent hiding (yield)
import Control.Applicative
import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
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
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Processor.Downstream"

type BoltState k v = Map.Map k v

runDownstream :: (Ord k, Monoid v, Topology t, Show k, Show v, SnapshotStore s)
              => ZKOptions
              -> ProcessorId
              -> t
              -> UserFormula k v
              -> s
              -> IO ()
runDownstream opts dId@(dName, _) topology uformula snapshotStore = do
    let (_, port) = addressFor topology dId
        ctype = processorType $ fromJust $ Map.lookup dName $
            processors topology
        producer = socketProducer uformula
        consumer zk mStateMVar =
            case ctype of
                Bolt ->
                    boltPipe dId zk uformula mStateMVar snapshotStore >->
                        downstreamPoolConsumer dName topology uformula
                Sink -> sinkConsumer uformula
                _ -> throw $ InvalidTopologyError $
                    dName ++ " is not a downstream processor"
        startState = case ctype of
                         Sink -> SinkRunning
                         _ -> UnspecifiedState
        processSocket s zk mStateMVar = runEffect $
            producer s >-> consumer zk mStateMVar

    registerProcessor opts dId startState $ \zk ->
        serve HostAny port $ \(s, _) ->
            injectMasterState zk (processSocket s zk)

    throw $ ZookeeperConnectionError $ "Unable to register downstream " ++ dName

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
         -> UserFormula k v
         -> MVar MasterState
         -> s
         -> Pipe (Payload k v) (Payload k v) IO ()
boltPipe bId@(bName, _) zk uFormula mStateMVar snapshotStore = do
    (stateMap', clk) <- lift $ restoreSnapshot snapshotStore bId $
        deserializeState uFormula
    let savedState | (Just stateMap) <- stateMap' = stateMap
                   | otherwise = Map.empty
    lift $ forceSetProcessorState zk bId $ BoltLoaded clk
    pipeLoop savedState Map.empty False clk
  where
    pipeLoop preSnapState postSnapState started loadedClock = do
        payload <- await

        -- If this is the first payload received, mark as Saved instead.
        unless started $
            lift $ forceSetProcessorState zk bId $ BoltSaved loadedClock

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

                if canSnapshot desiredSnapClock newLWM
                    then do
                        void <$> lift $ saveState bId zk stateA
                            (fromJust desiredSnapClock) snapshotStore
                        pipeLoop (stateA `mergeStates` stateB) Map.empty
                            True loadedClock
                    else pipeLoop stateA stateB True loadedClock

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

    canSnapshot (Just clk) lwm = lwm `clockGt` clk
    canSnapshot Nothing _ = False

saveState :: (Show k, Show v, SnapshotStore s)
          => ProcessorId
          -> ZK.Zookeeper
          -> BoltState k v
          -> Clock
          -> s
          -> IO ThreadId
saveState pId zk bState clk snapshotStore = forkOS $ do
    infoM $ "Saving snapshot for " ++ show pId
    saveSnapshot snapshotStore pId bState clk
    forceSetProcessorState zk pId (BoltSaved clk)

-- | Builds a Consumer that receives a payload emitted from a handle and
-- performs the sink operation defined in the given user formula.
sinkConsumer :: UserFormula k v -> Consumer (Payload k v) IO ()
sinkConsumer uformula = forever $ do
    payload <- await
    lift $ outputFn uformula $ payloadTuple payload
