module Hailstorm.Partition
( PartitionOffset(..)
, Partition
, Offset
, partitionFromFile
) where

import Control.Monad
import Pipes
import System.IO
import qualified Data.ByteString as B

type Offset = Integer
type Partition = String 

-- | An offset into a data partition.
data PartitionOffset = PartitionOffset B.ByteString Partition Offset
                       deriving (Eq, Show, Read)

-- | Creates a data partition from a given filesystem path. The returned
-- producer produces offsets into the partition.
partitionFromFile :: FilePath -> Producer PartitionOffset IO ()
partitionFromFile fp = do
    h <- lift $ openFile fp ReadMode
    partitionFromHandle h (show fp) 0

-- | Creates a data partition from a given handle. The returned producer
-- produces offsets into the partition.
partitionFromHandle :: Handle
                    -> String
                    -> Integer
                    -> Producer PartitionOffset IO ()
partitionFromHandle h partitionName offset = do
    eof <- lift $ hIsEOF h
    when eof $ lift (hSeek h AbsoluteSeek 0)
    l <- lift $ B.hGetLine h
    yield $ PartitionOffset l partitionName offset
    partitionFromHandle h partitionName (offset + 1)
