module Hailstorm.Clock
( Clock(..)
, Partition
, Offset
, extractClockMap
) where

import qualified Data.Map as Map

type Partition = String
type Offset = Integer

newtype Clock = Clock (Map.Map Partition Offset)
                deriving (Eq, Show, Read, Ord)

extractClockMap :: Clock -> Map.Map Partition Offset
extractClockMap (Clock clk) = clk
