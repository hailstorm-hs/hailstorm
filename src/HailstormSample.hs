module Main where

import Control.Concurrent
import Data.Monoid
import Hailstorm.Processors
import Hailstorm.UserFormula
import Pipes
import Pipes.Network.TCP

import qualified Data.ByteString.Char8 as C8
import qualified Pipes.Prelude as P

consumerPort :: String
consumerPort = "8000"

adderFormula :: UserFormula String (Sum Int)
adderFormula = UserFormula {
    convertFn = \x -> (C8.unpack x, Sum 1)
  , outputFn = \_ -> print "hi"
}

consumerLoop :: (Socket, SockAddr) -> IO ()
consumerLoop (s, sa) = do
    putStrLn $ "Switching consumer loop on " ++ show sa
    runEffect $ fromSocket s 4096 >-> P.map (\x -> C8.unpack x ++ "\n") >-> P.stdoutLn

consumerAcceptor :: IO ()
consumerAcceptor = serve HostAny consumerPort consumerLoop

main :: IO ()
main = do
    consumerId <- forkIO consumerAcceptor
    putStrLn $ "Spawned consumer thread " ++ show consumerId
    threadDelay 1000000
    let f = fileLineProducer "data/test.txt"
    let t = tupleSpoutProducer adderFormula f

    connect "127.0.0.1" consumerPort $ \(s,_) -> do
        let c = toSocket s
        runEffect $ t >-> P.map (C8.pack . show) >-> c
