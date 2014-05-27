module Hailstorm.InputSource
( InputSource(..)
, InputTuple(..)
, partitionIndex
) where

import Data.Maybe
import Data.List hiding (partition)
import Hailstorm.Clock
import Pipes
import qualified Data.ByteString as BS

data InputTuple = InputTuple BS.ByteString Partition Offset

class InputSource s where
    partitionProducer :: s -> Partition -> Offset -> Producer InputTuple IO ()
    allPartitions :: s -> IO [Partition]
    startClock :: s -> IO Clock

partitionIndex :: InputSource s => s -> Partition -> IO Int
partitionIndex s p = allPartitions s >>=
    \ps -> return $ fromJust $ elemIndex p ps
