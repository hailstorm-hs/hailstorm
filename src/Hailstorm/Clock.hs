module Hailstorm.Clock
( Clock(..)
, Partition
, Offset
) where

import qualified Data.Map as Map

type Partition = String
type Offset = Integer

newtype Clock = Clock (Map.Map Partition Offset)
                deriving (Eq, Show, Read)
