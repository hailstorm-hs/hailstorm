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

data ConsumerType = BoltConsumer | SinkConsumer



spoutStatePipe :: MVar MasterState -> Pipe (Payload k v) (Payload k v) IO ()
spoutStatePipe stateMVar = forever $ do
    ms <- lift $ readMVar stateMVar
    case ms of 
        GreenLight -> passOn
        _ -> do
            lift $ putStrLn $ "Spout waiting green light... master state=" ++ show ms
            lift $ threadDelay $ 1000 * 1000 * 10
    where passOn = await >>= yield

runSpoutFromProducer :: (Show k, Show v, Topology t)
                     => ZKOptions
                     -> String
                     -> t
                     -> UserFormula k v
                     -> Producer (Payload k v) IO ()
                     -> IO ()
runSpoutFromProducer zkOpts spoutId  topology uf producer = do
    registerProcessor zkOpts spoutId $ \zk -> do
        stateMVar <- newEmptyMVar
        _ <- forkOS $ pipeThread stateMVar

        monitorMasterState zk $ \et -> case et of
            (Left e) -> throw $ wrapInHSError e UnexpectedZookeeperError
            (Right ms) -> do 
                putStrLn $ "Spout: detected master state change to " ++ show ms
                tryTakeMVar stateMVar >> putMVar stateMVar ms -- Overwrite

    throw $ ZookeeperConnectionError
        "Spout zookeeper registration terminated unexpectedly"

    
    where pipeThread stateMVar = let downstream = downstreamConsumer spoutId topology uf in 
                       runEffect $ producer >-> spoutStatePipe stateMVar >-> downstream
            

        

runDownstream :: (Show k, Show v, Read k, Read v, Topology t)
              => ZKOptions
              -> (String, Int)
              -> t
              -> UserFormula k v
              -> IO ()
runDownstream opts (processorName, offset) topology uformula = do
    let (_, port) = addressFor topology (processorName, offset)
        ctype = consumerType (fromJust $
            Map.lookup processorName (processors topology))
        accepted socket = let sp = socketProducer uformula socket
                              fc = formulaConsumer uformula ctype
                          in runEffect $ sp >-> fc
    registerProcessor opts processorName $ \_ ->
        serve HostAny port $ \(s, _) -> accepted s

    throw $ ZookeeperConnectionError
        "Sink zookeeper registration terminated unexpectedly"

runNegotiator :: (Topology t) => ZKOptions -> t -> IO ()
runNegotiator zkOpts topology = do
    registerProcessor zkOpts "negotiator" $ \zk ->
        forceEitherIO
            (DuplicateNegotiatorError
                "Could not set state, probable duplicate process")
            (createMasterState zk Initialization) >> watchLoop zk

    throw $ ZookeeperConnectionError
        "Negotiator zookeeper registration terminated unexpectedly"

  where
    watchLoop zk = childrenWatchLoop zk "/living_processors" $ \children -> do
      putStrLn $ "Children changed to " ++ show children
      let expectedRegistrations = numProcessors topology + 1

      if length children < expectedRegistrations
        then putStrLn "Not enough children yet"
        else do
            _ <- forceEitherIO UnknownWorkerException (setMasterState zk Initialization)
            threadDelay $ 2 * 1000 * 1000
            _ <- forceEitherIO UnknownWorkerException (setMasterState zk GreenLight)
            putStrLn "Master state set to green light"


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
                   => String
                   -> t
                   -> UserFormula k v
                   -> Consumer (Payload k v) IO ()
downstreamConsumer processorId topology uf = dcInternal Map.empty
    where dcInternal connectionPool = do
            payload <- await
            -- TODO: figure out what payload actually is
            let sendAddresses = downstreamAddresses topology processorId payload
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
