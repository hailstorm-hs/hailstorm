module Main where

import Control.Concurrent
import Data.Monoid
import Hailstorm.Processors
import Hailstorm.UserFormula
import Network.Simple.TCP
import Pipes
import qualified Data.Map as Map

import qualified Data.ByteString.Char8 as C8
import qualified Pipes.Prelude as P

adderFormula :: UserFormula String (Sum Int)
adderFormula = UserFormula {
    convertFn = \x -> (C8.unpack x, Sum 1)
  , outputFn = \_ -> print "hi"
  , serialize = show
  , deserialize = read
}

topology :: HardcodedTopology
topology = HardcodedTopology (Map.fromList [
                ("spout", [("127.0.0.1", "10000")])
           ])

consumerAcceptor :: IO ()
consumerAcceptor = serve HostAny "10000" (\(s, _) -> consumerPrintLoop s)
    where consumerPrintLoop s = runEffect $ socketProducer adderFormula s >-> P.print

main :: IO ()
main = do
    consumerId <- forkIO consumerAcceptor
    putStrLn $ "Spawned consumer thread " ++ show consumerId
    threadDelay 1000000
    let f = fileLineProducer "data/test.txt"
    let t = tupleSpoutProducer adderFormula f
    let downstream = downstreamConsumer adderFormula "spout" topology
    runEffect $ t >-> downstream
