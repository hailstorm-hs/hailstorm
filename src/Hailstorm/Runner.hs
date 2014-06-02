module Hailstorm.Runner (runProcessors, localRunner) where

import Control.Concurrent
import Control.Monad
import Data.Maybe
import Data.Monoid
import Hailstorm.Concurrency
import Hailstorm.InputSource
import Hailstorm.Logging
import Hailstorm.Negotiator
import Hailstorm.Processor
import Hailstorm.Processor.Downstream
import Hailstorm.Processor.Spout
import Hailstorm.SnapshotStore
import Hailstorm.Topology.HardcodedTopology
import Hailstorm.UserFormula
import Hailstorm.ZKCluster
import System.Exit
import System.IO
import qualified Data.Map as Map
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Runner"

errorM :: String -> IO ()
errorM = L.errorM "Hailstorm.Runner"

-- Runs a list of processors on this host
runProcessors :: (Show k, Show v, Read k, Read v, 
                  Ord k, Monoid v, SnapshotStore s, InputSource i)
              => ZKOptions
              -> HardcodedTopology
              -> UserFormula k v
              -> i
              -> s
              -> [ProcessorId]
              -> IO ()

runProcessors zkOpts topology uFormula inputSource snapshotStore pids = do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering
    initializeLogging
    quietZK

    tids <- forM pids startProcessor
    forever $ do
        forM_ tids $ \tid -> do
            dead <- threadDead tid
            when dead $ do
                errorM $ "Processor thread terminated: shutting down"
                exitWith $ ExitFailure 1
        threadDelay $ 1000 * 1000

    where
        startProcessor :: ProcessorId -> IO (ThreadId)
        startProcessor ("negotiator", 0) = do
            infoM "Starting negotiator thread ..."
            forkOS $ runNegotiator zkOpts topology

        startProcessor pid@(pName, pInstance) = do
            let processor = (processorMap topology) Map.!  pName
            case (processorType processor) of
                Spout ->  do
                    partition <- indexToPartition inputSource pInstance
                    infoM $ "Spawning spout for partition '" ++ partition ++ "'"
                    forkOS $ runSpout zkOpts pName partition topology inputSource uFormula
                Bolt -> do
                    infoM $ "Spawning bolt '" ++ pName ++ "'"
                    forkOS $ runDownstream zkOpts pid topology uFormula inputSource snapshotStore
                Sink -> do
                    infoM $ "Spawning sink '" ++ pName ++ "'"
                    forkOS $ runDownstream zkOpts pid topology uFormula inputSource snapshotStore

-- TODO: this needs to be cleaned out. Currently hardcoded.
-- TODO: I'm making this hardcoded topology-specific pending
-- discussion of new interface method to add to Topology
localRunner :: ( Show k, Show v, Read k, Read v
               , Ord k, Monoid v, InputSource i, SnapshotStore s)
            => ZKOptions
            -> HardcodedTopology
            -> UserFormula k v
            -> ProcessorName
            -> i
            -> s
            -> IO ()
localRunner zkOpts topology formula spoutId source snapshotStore = do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering
    initializeLogging
    quietZK

    infoM "Running in local mode"

    negotiatorTid <- forkOS $ runNegotiator zkOpts topology
    infoM $ "Spawned negotiator " ++ show negotiatorTid
    threadDelay 1000000

    let runDownstreamThread processorTuple = do
            downstreamTid <- forkOS $ runDownstream zkOpts processorTuple
                topology formula source snapshotStore
            infoM $ "Spawned downstream " ++ show downstreamTid
            return downstreamTid
    downstreamTids <- mapM runDownstreamThread $ (Map.keys . addresses) topology
    threadDelay 1000000

    spoutPartition <- allPartitions source >>= return . head
    infoM $ "Spout partition will be " ++ show spoutPartition
    spoutTid <- forkOS $ runSpout zkOpts spoutId spoutPartition topology source formula

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
