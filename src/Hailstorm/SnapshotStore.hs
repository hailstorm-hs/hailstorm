module Hailstorm.SnapshotStore
( SnapshotStore(..)
) where

import Hailstorm.Clock
import Hailstorm.Processor
import Hailstorm.TransactionTypes

class SnapshotStore s where
    saveSnapshot :: s
                 -> ProcessorId
                 -> BoltState
                 -> (BoltState -> String)
                 -> Clock
                 -> IO ()

    restoreSnapshot :: s
                    -> ProcessorId
                    -> (String -> BoltState)
                    -> IO (Maybe BoltState, Clock)
