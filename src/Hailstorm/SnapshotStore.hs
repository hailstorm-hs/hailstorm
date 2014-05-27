module Hailstorm.SnapshotStore
( SnapshotStore(..)
) where

import Hailstorm.Clock
import Hailstorm.Processor
import qualified Data.Map as Map

class SnapshotStore s where
    saveSnapshot :: (Show k, Show v)
                 => s
                 -> ProcessorId
                 -> Map.Map k v
                 -> Clock
                 -> IO ()

    restoreSnapshot :: s
                    -> ProcessorId
                    -> (String -> Map.Map k v)
                    -> IO (Map.Map k v, Clock)
