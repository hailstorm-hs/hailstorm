{-# LANGUAGE OverloadedStrings #-}

module Hailstorm.Processor.Downstream
( runDownstream
) where

import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
import Data.Maybe
import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.MasterState
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Processor.Pool
import Hailstorm.Topology
import Hailstorm.ZKCluster
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map as Map

type BoltState k v = Map.Map k v

runDownstream :: (Ord k, Monoid v, Topology t)
              => ZKOptions
              -> ProcessorId
              -> t
              -> UserFormula k v
              -> IO ()
runDownstream opts dId@(dName, _) topology uformula = do
    let (_, port) = addressFor topology dId
        ctype = processorType $ fromJust $ Map.lookup dName $
            processors topology
        producer = socketProducer uformula
        consumer mStateMVar =
            case ctype of
                Bolt -> boltPipe dName uformula mStateMVar Map.empty Map.empty >->
                    downstreamPoolConsumer dName topology uformula
                Sink -> sinkConsumer uformula
                _ -> throw $ InvalidTopologyError $
                    dName ++ " is not a downstream processor"
        processSocket s mStateMVar = runEffect $
            producer s >-> consumer mStateMVar

    registerProcessor opts dId SinkRunning $ \zk ->
        serve HostAny port $ \(s, _) ->
            injectMasterState zk (processSocket s)

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
boltPipe :: (Ord k, Monoid v)
         => ProcessorName
         -> UserFormula k v
         -> MVar MasterState
         -> BoltState k v
         -> BoltState k v
         -> Pipe (Payload k v) (Payload k v) IO ()
boltPipe bName uformula mStateMVar preSnapshotState postSnapshotState = do
    payload <- await

    let (key, val) = payloadTuple payload
        (partition, offset) = payloadPosition payload
        lwmMap = payloadLowWaterMarkMap payload
        newLWM = buildLWM lwmMap
        newLWMMap = Map.union (Map.singleton bName newLWM) lwmMap

    let passOn stateA stateB nextSnapshotClock = do
            let valA = Map.findWithDefault mempty key stateA
                valB = Map.findWithDefault mempty key stateB
                canSnapshot (Just clk) = newLWM `isNewerClock` clk
                canSnapshot Nothing = False

            yield Payload { payloadTuple = (key, valA `mappend` valB)
                          , payloadPosition = (partition, offset)
                          , payloadLowWaterMarkMap = newLWMMap
                          }

            if canSnapshot nextSnapshotClock
                then do
                    lift $ saveSnapshot stateA $ fromJust nextSnapshotClock
                    boltPipe bName uformula mStateMVar
                        (stateA `mergeStates` stateB) Map.empty
                else boltPipe bName uformula mStateMVar stateA stateB

    -- Determine next snapshot clock, if available.
    mState <- lift $ readMVar mStateMVar
    case getNextSnapshotClock mState of
        Just desiredClock@(Clock clockMap) -> do
            let desiredOffset = clockMap Map.! partition
            if offset > desiredOffset
                then let b' = mergeWithTuple postSnapshotState (key, val)
                     in passOn preSnapshotState b' (Just desiredClock)
                else let a' = mergeWithTuple preSnapshotState (key, val)
                     in passOn a' postSnapshotState (Just desiredClock)
        Nothing ->
            let mergedState = preSnapshotState `mergeStates` postSnapshotState
            in passOn (mergeWithTuple mergedState (key, val)) Map.empty Nothing
  where
    getNextSnapshotClock (Flowing (Just clk)) = Just clk
    getNextSnapshotClock _ = Nothing
    mergeWithTuple st (key, val) = Map.unionWith mappend st $
        Map.singleton key val
    mergeLWM = Map.unionWith min
    mergeStates = Map.unionWith mappend
    buildLWM lwmMap = Clock $ foldr (mergeLWM . extractClockMap) Map.empty $
        Map.elems lwmMap
    isNewerClock (Clock clk1) (Clock clk2) =
        let (x:xs) `gtElems` (y:ys) = x > y && xs `gtElems` ys
            [] `gtElems` [] = True
            _ `gtElems` _ = False
        in (Map.keys clk1 == Map.keys clk2) &&
            (Map.elems clk1 `gtElems` Map.elems clk2)

-- TODO: fork new OS thread and save to disk
saveSnapshot :: BoltState k v -> Clock -> IO ()
saveSnapshot _ _ = return ()

-- | Builds a Consumer that receives a payload emitted from a handle and
-- performs the sink operation defined in the given user formula.
sinkConsumer :: UserFormula k v -> Consumer (Payload k v) IO ()
sinkConsumer uformula = forever $ do
    payload <- await
    lift $ outputFn uformula $ payloadTuple payload
