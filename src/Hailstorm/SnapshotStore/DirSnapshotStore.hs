module Hailstorm.SnapshotStore.DirSnapshotStore
( DirSnapshotStore(..)
) where

import Hailstorm.Clock
import Hailstorm.Processor
import Hailstorm.SnapshotStore
import Data.List.Split
import System.Directory
import System.FilePath
import qualified Data.Map as Map

import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.SnapshotStore.DirSnapshotStore"

data DirSnapshotStore = DirSnapshotStore FilePath
                        deriving (Eq, Show, Read)

instance SnapshotStore DirSnapshotStore where

    saveSnapshot (DirSnapshotStore dir) pId bState stateSerializeFn clk = do
        createDirectoryIfMissing True dir
        infoM $ "Saving bolt " ++ show pId
        writeFile (dir </> genStoreFilename pId) $
            stateSerializeFn bState ++ "\1" ++ show clk

    restoreSnapshot (DirSnapshotStore dir) pId stateDeserializeFn = do
        let fname = dir </> genStoreFilename pId
        exists <- doesFileExist fname
        if exists
            then do
                infoM $ "Restoring bolt " ++ show pId
                contents <- readFile (dir </> genStoreFilename pId)
                let [bStateS, clkS] = splitOn "\1" contents
                    clk = read clkS :: Clock
                    bState = stateDeserializeFn bStateS
                return (Just bState, clk)
            else do
                infoM $ "No snapshot found for bolt " ++ show pId
                return (Nothing, Clock Map.empty)

genStoreFilename :: ProcessorId -> FilePath
genStoreFilename (pName, pInstance) = pName ++ "-" ++ show pInstance
