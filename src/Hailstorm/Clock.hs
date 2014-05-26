module Hailstorm.Clock
( Clock(..)
) where

import Hailstorm.InputSource
import qualified Data.Map as Map

newtype Clock = Clock (Map.Map Partition Offset)
                deriving (Eq, Show, Read)
