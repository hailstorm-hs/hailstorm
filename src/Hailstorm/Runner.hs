module Hailstorm.Runner (localRunner) where

import Control.Concurrent
import Hailstorm.Processor.Runner
import Hailstorm.Topology
import Hailstorm.UserFormula
import Hailstorm.Partition
import Hailstorm.Payload
import Hailstorm.ZKCluster

-- TODO: this needs to be cleaned out. Currently hardcoded.
localRunner :: (Topology t, Show t, Show k, Show v, Read k, Read v)
            => ZKOptions
            -> t
            -> UserFormula k v
            -> FilePath
            -> String
            -> String
            -> IO ()
localRunner zkOpts topology formula filename ispout fsink = do
    putStrLn "Running in local mode..."
    consumerId <- forkIO $ runSink zkOpts (fsink, 0) topology formula
    putStrLn $ "Spawned sink " ++ show consumerId
    threadDelay 1000000
    let f = partitionFromFile filename
    runSpoutFromProducer ispout topology formula
      (partitionToPayloadProducer formula f)
