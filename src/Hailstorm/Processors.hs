module Hailstorm.Processors where

import Control.Monad
import Hailstorm.UserFormula
import Pipes
import System.IO
import qualified Data.ByteString as B

data DataPartitionOffset = DataPartitionOffset B.ByteString String Integer
    deriving (Eq, Show, Read)

data TuplePartitionOffset k v = TuplePartitionOffset (k,v) String Integer
    deriving (Eq, Show, Read)

tupleSpoutProducer :: (Show k, Show v, Monad m) => UserFormula k v -> Producer DataPartitionOffset m () -> Producer (TuplePartitionOffset k v) m ()
tupleSpoutProducer uf producer = for producer (\x -> case x of (DataPartitionOffset bs p o) ->  yield (TuplePartitionOffset (convertFn uf bs) p o))


handleLineProducer :: Handle -> String -> Integer -> Producer DataPartitionOffset IO ()
handleLineProducer h partitionName offset = do
    eof <- lift $ hIsEOF h
    when eof $ lift (hSeek h AbsoluteSeek 0)
    l <- lift $ B.hGetLine h
    yield $ DataPartitionOffset l partitionName offset
    handleLineProducer h partitionName (offset + 1)


fileLineProducer :: FilePath -> Producer DataPartitionOffset IO ()
fileLineProducer fp = do
    h <- lift $ openFile fp ReadMode
    handleLineProducer h (show fp) 0
