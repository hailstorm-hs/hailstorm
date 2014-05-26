module Hailstorm.MasterState
( MasterState(..)
) where

import Hailstorm.Clock

data MasterState = Unavailable
                 | Initialization
                 | SpoutsPaused
                 | Flowing (Maybe Clock)
                   deriving (Eq, Read, Show)
