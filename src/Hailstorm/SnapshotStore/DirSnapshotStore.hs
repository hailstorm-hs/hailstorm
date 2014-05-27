module Hailstorm.SnapshotStore.DirSnapshotStore
( DirSnapshotStore(..)
) where

import Hailstorm.Clock
import Hailstorm.Processor
import Hailstorm.SnapshotStore
import Data.List.Split
import System.Directory
import System.FilePath

data DirSnapshotStore = DirSnapshotStore FilePath
                        deriving (Eq, Show, Read)

instance SnapshotStore DirSnapshotStore where

    saveSnapshot (DirSnapshotStore dir) pId bState clk = do
        createDirectoryIfMissing True dir
        writeFile (dir </> genStoreFilename pId) $
            show bState ++ "\1" ++ show clk

    restoreSnapshot (DirSnapshotStore dir) pId stateDeserializer = do
        contents <- readFile (dir </> genStoreFilename pId)
        let [bStateS, clkS] = splitOn "\1" contents
            clk = read clkS :: Clock
            bState = stateDeserializer bStateS
        return (bState, clk)

genStoreFilename :: ProcessorId -> FilePath
genStoreFilename (pName, pInstance) = pName ++ "-" ++ show pInstance
