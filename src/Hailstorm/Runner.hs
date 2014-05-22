module Hailstorm.Runner (localRunner) where

import Control.Concurrent
import Control.Monad
import Data.Maybe
import GHC.Conc (threadStatus, ThreadStatus(..))
import Hailstorm.Partition
import Hailstorm.Payload
import Hailstorm.Processor.Runner
import Hailstorm.Topology
import Hailstorm.UserFormula
import Hailstorm.ZKCluster
import System.Exit
import System.IO

import qualified Data.Map as Map

threadDead :: ThreadId -> IO Bool
threadDead = fmap (\x -> x == ThreadDied || x == ThreadFinished) . threadStatus

-- TODO: this needs to be cleaned out. Currently hardcoded.
localRunner :: (Topology t, Show t, Show k, Show v, Read k, Read v)
            => ZKOptions
            -> t
            -> UserFormula k v
            -> FilePath
            -> String
            -> String
            -> IO ()

localRunner zkOpts topology formula filename ispout _ = do
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering
    quietZK

    putStrLn "Running in local mode..."

    negotiatorId <- forkOS $ runNegotiator zkOpts topology
    putStrLn $ "Spawned negotiator" ++ show negotiatorId
    threadDelay 1000000

    sinkId <- forkOS $ runSink zkOpts ("sink", 0) topology formula
    putStrLn $ "Spawned sink " ++ show sinkId
    threadDelay 1000000

    let f = partitionFromFile filename
    spoutId <- forkOS $ runSpoutFromProducer zkOpts ispout topology formula
      (partitionToPayloadProducer formula f)

    let threadToName = Map.fromList [
                     (negotiatorId, "Negotiator")
                   , (spoutId, "Spout")
                   , (sinkId, "Sink")
                   ]

    forever $ do 
        mapM_ (\tid -> do
                dead <- threadDead tid
                when dead $ do
                    hPutStrLn stderr $ (fromJust $ Map.lookup tid threadToName) ++ " thread terminated... shutting down"
                    exitWith $ ExitFailure 1
              ) (Map.keys threadToName)
        threadDelay $ 1000 * 1000
