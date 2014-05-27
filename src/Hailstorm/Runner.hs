module Hailstorm.Runner (localRunner) where

import Control.Concurrent
import Control.Monad
import Data.Maybe
import Data.Monoid
import Hailstorm.Concurrency
import Hailstorm.InputSource.FileSource
import Hailstorm.Negotiator
import Hailstorm.Processor.Spout
import Hailstorm.Processor.Downstream
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

-- TODO: this needs to be cleaned out. Currently hardcoded.
-- TODO: I'm making this hardcoded topology-specific pending
-- discussion of new interface method to add to Topology
localRunner :: (Show k, Show v, Read k, Read v, Ord k, Monoid v)
            => ZKOptions
            -> HardcodedTopology
            -> UserFormula k v
            -> FilePath
            -> String
            -> IO ()
localRunner zkOpts topology formula filename ispout = do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering
    quietZK
    let source = FileSource [filename]

    infoM "Running in local mode"

    negotiatorId <- forkOS $ runNegotiator zkOpts topology source
    infoM $ "Spawned negotiator " ++ show negotiatorId
    threadDelay 1000000

    let runDownstreamThread processorTuple = do
            downstreamId <- forkOS $ runDownstream zkOpts processorTuple
                topology formula
            infoM $ "Spawned downstream " ++ show downstreamId
            return downstreamId
    downstreamIds <- mapM runDownstreamThread $ (Map.keys . addresses) topology
    threadDelay 1000000

    spoutId <- forkOS $ runSpout zkOpts ispout filename topology source formula

    let baseThreads = [(negotiatorId, "Negotiator"), (spoutId, "Spout")]
        consumerThreads = map (\tid -> (tid, show tid)) downstreamIds
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
