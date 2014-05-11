module Main where

import Data.Monoid
import Hailstorm.Processors
import Hailstorm.UserFormula

adderFormula :: UserFormula String (Sum Int)
adderFormula = UserFormula {
    convertFn = \x -> ("bieber", Sum 1)
  , outputFn = \_ -> print "hi"
}

main = do
    print "Hello world"
    runSpout adderFormula
