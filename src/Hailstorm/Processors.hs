module Hailstorm.Processors where

import Hailstorm.UserFormula

data Spout k v = Spout (UserFormula k v)
