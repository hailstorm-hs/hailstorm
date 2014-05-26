module Hailstorm.MasterState
( MasterState(..)
) where

import Hailstorm.Clock

data MasterState = Unavailable
                 | Initialization
                 | SpoutPause
                 | GreenLight Clock
                   deriving (Eq, Read, Show)
