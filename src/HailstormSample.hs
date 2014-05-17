module Main where

import Control.Concurrent
import Data.Monoid
import Hailstorm.Processors
import Hailstorm.UserFormula

import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

adderFormula :: UserFormula String (Sum Int)
adderFormula = newUserFormula 
    (\x -> (C8.unpack x, Sum 1))
    (\(k, v) -> print (k, v))

topology :: HardcodedTopology
topology = HardcodedTopology (Map.fromList [
                ("spout", [("127.0.0.1", "10000")])
           ])

main :: IO ()
main = do
    consumerId <- forkIO $ runSink "sink" "10000" topology adderFormula
    putStrLn $ "Spawned sink " ++ show consumerId
    threadDelay 1000000
    let f = fileLineProducer "data/test.txt"
    runSpoutFromProducer "spout" topology adderFormula (dataToPayloadPipe adderFormula f)
