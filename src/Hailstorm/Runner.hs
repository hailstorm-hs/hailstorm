module Hailstorm.Runner where

import Control.Concurrent
import Data.Monoid
import Hailstorm.Processors
import Hailstorm.UserFormula

import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map
import qualified Hailstorm.Zookeeper as HSZK

sampleAddFormula :: UserFormula String (Sum Int)
sampleAddFormula = newUserFormula 
    (\x -> (C8.unpack x, Sum 1))
    (\(k, v) -> print (k, v))

sampleTopology :: HardcodedTopology
sampleTopology = HardcodedTopology {
    processorMap = mkProcessorMap [
        Spout "spout" 1 ["sink"]
      , Sink "sink" 1
    ]
  , addresses = (Map.fromList [
        (("sink", 0), ("127.0.0.1", "10000"))
    ])
}

localRunner :: (Topology t, Show t, Show k, Show v, Read k, Read v) => HSZK.ZKOptions -> t -> UserFormula k v -> IO ()
localRunner zkOpts topology formula = do
    putStrLn $ "Running in local mode..." 
    putStrLn $ (show topology)
    consumerId <- forkIO $ runSink zkOpts ("sink", 0) topology formula
    putStrLn $ "Spawned sink " ++ show consumerId
    threadDelay 1000000
    let f = fileLineProducer "data/test.txt"
    runSpoutFromProducer "spout" topology formula (dataToPayloadPipe formula f)
