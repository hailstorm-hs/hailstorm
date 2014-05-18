module Hailstorm.Clock
( Clock(..)
) where

import qualified Data.Map as Map

newtype Clock = Clock (Map.Map String Integer)
                deriving (Eq, Show, Read)
