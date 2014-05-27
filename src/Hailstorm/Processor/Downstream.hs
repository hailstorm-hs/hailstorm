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
                Bolt -> boltPipe uformula mStateMVar Map.empty Map.empty >->
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
         => UserFormula k v
         -> MVar MasterState
         -> BoltState k v
         -> BoltState k v
         -> Pipe (Payload k v) (Payload k v) IO ()
boltPipe uformula mStateMVar preSnapshotState postSnapshotState = do
    payload <- await

    let (key, val) = payloadTuple payload
        (partition, offset) = payloadPosition payload
        calcLowWaterMarkMap = id -- TODO: calculate
        mergeWithVal st = Map.unionWith mappend st $ Map.singleton key val
        mergeStates = Map.unionWith mappend
        passOnWithSnapshot stateA stateB = do
            let valA = Map.findWithDefault mempty key stateA
                valB = Map.findWithDefault mempty key stateB
            yield Payload { payloadTuple = (key, valA `mappend` valB)
                          , payloadPosition = payloadPosition payload
                          , payloadLowWaterMarkMap = calcLowWaterMarkMap $
                              payloadLowWaterMarkMap payload
                          }
            -- TODO: save snapshot.
            boltPipe uformula mStateMVar stateA stateB
        passOnNoSnapshot st = do
            let val' = Map.findWithDefault mempty key st
            yield Payload { payloadTuple = (key, val')
                          , payloadPosition = payloadPosition payload
                          , payloadLowWaterMarkMap = calcLowWaterMarkMap $
                              payloadLowWaterMarkMap payload
                          }
            boltPipe uformula mStateMVar st Map.empty

    -- Determine next snapshot clock, if available.
    mState <- lift $ readMVar mStateMVar
    case mState of
        Flowing mClock ->
            case mClock of
                Just (Clock nextSnapshotMap) -> do
                    let desiredOffset = nextSnapshotMap Map.! partition
                    if offset > desiredOffset
                        then passOnWithSnapshot preSnapshotState
                            (mergeWithVal postSnapshotState)
                        else passOnWithSnapshot (mergeWithVal preSnapshotState)
                            postSnapshotState
                Nothing -> passOnNoSnapshot $ mergeWithVal
                    (preSnapshotState `mergeStates` postSnapshotState)

        _ -> passOnNoSnapshot $ mergeWithVal
            (preSnapshotState `mergeStates` postSnapshotState)

-- | Builds a Consumer that receives a payload emitted from a handle and
-- performs the sink operation defined in the given user formula.
sinkConsumer :: UserFormula k v -> Consumer (Payload k v) IO ()
sinkConsumer uformula = forever $ do
    payload <- await
    lift $ outputFn uformula $ payloadTuple payload
