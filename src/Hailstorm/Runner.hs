module Hailstorm.Runner (runProcessors, localRunner) where

import Control.Concurrent
import Control.Monad
import Data.Maybe
import Hailstorm.Concurrency
import Hailstorm.InputSource
import Hailstorm.Logging
import Hailstorm.Negotiator
import Hailstorm.Processor
import Hailstorm.Processor.Downstream
import Hailstorm.Processor.Spout
import Hailstorm.SnapshotStore
import Hailstorm.Topology
import Hailstorm.Topology.HardcodedTopology
import Hailstorm.ZKCluster
import System.Exit
import System.IO
import qualified Data.Map as Map
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Runner"

errorM :: String -> IO ()
errorM = L.errorM "Hailstorm.Runner"

-- Runs a list of processors on this host.
runProcessors :: (SnapshotStore s, InputSource i)
              => ZKOptions
              -> HardcodedTopology
              -> i
              -> s
              -> [ProcessorId]
              -> IO ()
runProcessors zkOpts topology inputSource snapshotStore pids = do
    prepareRunner

    tids <- forM pids startProcessor
    unless (null pids) $ forever $ do
        forM_ tids $ \tid -> do
            dead <- threadDead tid
            when dead $ do
                errorM "Processor thread terminated: shutting down"
                exitWith $ ExitFailure 1
        threadDelay $ 1000 * 1000

  where
    startProcessor :: ProcessorId -> IO ThreadId
    startProcessor ("negotiator", 0) = do
        infoM "Starting negotiator thread ..."
        forkOS $ runNegotiator zkOpts topology

    startProcessor pid@(pName, pInstance) = do
        let pr = lookupProcessorWithFailure pName topology
        case pr of
            SpoutNode s ->  do
                partition <- indexToPartition inputSource pInstance
                infoM $ "Spawning spout for partition '" ++ partition ++ "'"
                forkOS $ runSpout zkOpts s partition topology inputSource
            BoltNode _ -> do
                infoM $ "Spawning bolt '" ++ pName ++ "'"
                forkOS $ runDownstream zkOpts pid topology inputSource snapshotStore
            SinkNode _ -> do
                infoM $ "Spawning sink '" ++ pName ++ "'"
                forkOS $ runDownstream zkOpts pid topology inputSource snapshotStore

localRunner :: (SnapshotStore s, InputSource i)
            => ZKOptions
            -> HardcodedTopology
            -> ProcessorName
            -> i
            -> s
            -> IO ()
localRunner zkOpts topology spName source snapshotStore = do
    infoM "Running in local mode"
    prepareRunner

    negotiatorTid <- forkOS $ runNegotiator zkOpts topology
    infoM $ "Spawned negotiator " ++ show negotiatorTid
    threadDelay 1000000

    let runDownstreamThread processorTuple = do
            downstreamTid <- forkOS $ runDownstream zkOpts processorTuple
                topology source snapshotStore
            infoM $ "Spawned downstream " ++ show downstreamTid
            return downstreamTid
    downstreamTids <- mapM runDownstreamThread $ (Map.keys . addresses) topology
    threadDelay 1000000

    spoutPartition <- liftM head (allPartitions source)
    infoM $ "Spout partition will be " ++ show spoutPartition
    let sp' = lookupProcessorWithFailure spName topology
    spoutTid <-
        case sp' of
            SpoutNode sp -> forkOS $ runSpout zkOpts sp spoutPartition topology source
            _ -> error $ spName ++ " is not a spout"

    let baseThreads = [(negotiatorTid, "Negotiator"), (spoutTid, "Spout")]
        consumerThreads = map (\tid -> (tid, show tid)) downstreamTids
        threadToName = Map.fromList $ baseThreads ++ consumerThreads

    forever $ do
        mapM_ (\tid -> do
                dead <- threadDead tid
                when dead $ do
                    errorM $ fromJust (Map.lookup tid threadToName) ++
                        " thread terminated: shutting down"
                    exitWith $ ExitFailure 1
              ) (Map.keys threadToName)
        threadDelay $ 1000 * 1000

prepareRunner :: IO ()
prepareRunner = do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering
    initializeLogging
    quietZK
