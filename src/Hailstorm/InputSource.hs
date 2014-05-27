module Hailstorm.InputSource
( InputSource(..)
, InputTuple(..)
, FileSource(..)
, Partition
, Offset
, partitionIndex
) where

import Control.Monad
import Data.Maybe
import Data.List hiding (partition)
import Hailstorm.Clock
import Pipes
import System.IO

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

data InputTuple = InputTuple BS.ByteString Partition Offset

class InputSource s where
    partitionProducer :: s -> Partition -> Offset -> Producer InputTuple IO ()
    allPartitions :: s -> IO [Partition]
    startClock :: s -> IO (Clock)

partitionIndex :: (InputSource s) => s -> Partition -> IO Int
partitionIndex s p = allPartitions s  >>= \ps -> return $ fromJust $ elemIndex p ps

data FileSource = FileSource [FilePath]
    deriving (Eq, Show, Read)

instance InputSource FileSource where
    partitionProducer _ partition offset = do
        h <- lift $ openFile partition ReadMode
        numLines <- lift $ lineCount h
        lift $ hClose h

        let seekLines = fromInteger offset `mod` numLines
        h2 <- lift $ openFile partition ReadMode
        lift $ discardNLines seekLines h2

        cyclicalHandleProducer h2 partition offset

    allPartitions (FileSource paths) = return $ sort paths
    startClock s = allPartitions s >>= \ps ->  return $ Clock $ Map.fromList $ zip ps (repeat 0) 


-- | Quickly counts the number of lines in a file
lineCount :: Handle -> IO Int
lineCount h = C8.hGetContents h  >>= \c -> return (C8.count '\n' c)

-- | Discards n lines from a handle
discardNLines :: Int -> Handle -> IO ()
discardNLines n h = replicateM_ n (C8.hGetLine h)

-- | Yields tuples from a file cyclically
cyclicalHandleProducer :: Handle
                       -> String
                       -> Integer
                       -> Producer InputTuple IO ()
cyclicalHandleProducer h partitionName offsetCtr = do
    eof <- lift $ hIsEOF h
    when eof $ lift (hSeek h AbsoluteSeek 0)
    l <- lift $ BS.hGetLine h
    yield $ InputTuple l partitionName offsetCtr
    cyclicalHandleProducer h partitionName (offsetCtr + 1)
