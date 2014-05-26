module Hailstorm.MasterState
( MasterState(..)
) where

import Hailstorm.Clock

data MasterState = Unavailable
                 | Initialization
                 | ValveClosed
                 | ValveOpened Clock
                   deriving (Eq, Read, Show)
