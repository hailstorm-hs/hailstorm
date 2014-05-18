module Hailstorm.Sample.WordCountSample where

import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Topology
import Hailstorm.Processor
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

wordCountFormula :: UserFormula String (Sum Int)
wordCountFormula = newUserFormula
    (\x -> (C8.unpack x, Sum 1))
    (\(k, v) -> print (k, v))

wordCountTopology :: HardcodedTopology
wordCountTopology = HardcodedTopology
  {
      processorMap = mkProcessorMap
      [ Spout "words" 1 ["sink"]
      , Sink "sink" 1
      ]
      , addresses = Map.fromList
      [ (("sink", 0), ("127.0.0.1", "10000"))
      ]
  }
