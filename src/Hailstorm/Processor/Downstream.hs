{-# LANGUAGE OverloadedStrings #-}

module Hailstorm.Processor.Downstream
( runDownstream
) where

import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
import Data.List.Split
import Data.Maybe
import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Clock
import Hailstorm.Error
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
        consumer =
            case ctype of
                Bolt -> boltPipe uformula Map.empty >->
                    downstreamPoolConsumer dName topology uformula
                Sink -> sinkConsumer uformula
                _ -> throw $ InvalidTopologyError $
                    dName ++ " is not a downstream processor"
        processSocket s = runEffect $ producer s >-> consumer
    registerProcessor opts dId SinkRunning $ const $ serve HostAny port $
        \(s, _) -> processSocket s
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
    lift $ outputFn uformula $ payloadTuple payload
