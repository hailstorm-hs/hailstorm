module Hailstorm.Runner (localRunner) where

import Control.Concurrent
import Control.Monad
import Data.Maybe
import Hailstorm.Concurrency
import Hailstorm.InputSource.FileSource
import Hailstorm.Negotiator
import Hailstorm.Processor
import Hailstorm.Processor.Spout
import Hailstorm.Processor.Downstream
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

-- TODO: this needs to be cleaned out. Currently hardcoded.
-- TODO: I'm making this hardcoded topology-specific pending
-- discussion of new interface method to add to Topology
localRunner :: (SnapshotStore s)
            => ZKOptions
            -> HardcodedTopology
            -> FilePath
            -> ProcessorName
            -> s
            -> IO ()
localRunner zkOpts topology filename spName snapshotStore = do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering
    quietZK
    let source = FileSource [filename]

    infoM "Running in local mode"

    negotiatorTid <- forkOS $ runNegotiator zkOpts topology source
    infoM $ "Spawned negotiator " ++ show negotiatorTid
    threadDelay 1000000

    let runDownstreamThread processorTuple = do
            downstreamTid <- forkOS $ runDownstream zkOpts processorTuple
                topology snapshotStore
            infoM $ "Spawned downstream " ++ show downstreamTid
            return downstreamTid
    downstreamTids <- mapM runDownstreamThread $ (Map.keys . addresses) topology
    threadDelay 1000000

    let sp' = fromJust $ lookupProcessor spName topology
    spoutTid <-
        case sp' of
            SpoutNode sp -> forkOS $ runSpout zkOpts sp 0 filename topology source
            _ -> error "given name is not a spout"

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
