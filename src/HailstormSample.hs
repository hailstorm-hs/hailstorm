module Main where

import Data.Monoid
import Hailstorm.Processors
import Hailstorm.UserFormula
import Pipes

import qualified Data.ByteString.Char8 as C8

adderFormula :: UserFormula String (Sum Int)
adderFormula = UserFormula {
    convertFn = \x -> (C8.unpack x, Sum 1)
  , outputFn = \_ -> print "hi"
}

main :: IO ()
main = do
    print "Hello world"
    let f = fileLineProducer "data/test.txt"
    let t = tupleSpoutProducer adderFormula f
    runEffect $ for t (lift . print)
