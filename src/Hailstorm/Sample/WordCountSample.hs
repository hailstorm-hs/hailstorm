module Hailstorm.Sample.WordCountSample
( wordCountFormula
, wordCountTopology
) where

import Data.Monoid
import Hailstorm.UserFormula
import Hailstorm.Topology
import Hailstorm.Processor
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

localServer :: String
localServer = "127.0.0.1"

wordCountFormula :: UserFormula String (Sum Int)
wordCountFormula = newUserFormula
    (\x -> (C8.unpack x, Sum 1))
    (\_ -> return ())

wordCountTopology :: HardcodedTopology
wordCountTopology = HardcodedTopology
  {
      processorMap = mkProcessorMap
      [ Spout "words" 1 ["count"]
      , Bolt "count" 1 ["sink"]
      , Sink "sink" 1
      ]
      , addresses = Map.fromList
      [ (("sink", 0), (localServer, "10000"))
      , (("count", 0), (localServer, "10001"))
      ]
  }
