module Hailstorm.Sample.WordCountKafkaEmitter 
    ( emitLinesForever
    ) where

import Control.Monad
import Control.Monad.Loops
import Data.List.Split
import Hailstorm.InputSource.KafkaSource
import Haskakafka
import System.IO

import qualified System.Log.Logger as L
import qualified Data.ByteString.Char8 as BS

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Sample/WordCountKafkaEmitter"

emitLinesForever :: FilePath -> KafkaOptions -> Int -> Int -> IO ()
emitLinesForever fp kOpts emitSleepMs emitSizeBatch = do
    infoM "Constructing kafka"
    (Right (kafka, kTopic)) <- kafkaFromOptions kOpts KafkaProducer
    infoM "Constructed kafka, beginning emit loop"

    emitLoop kafka kTopic 

    where 
        emitLoop kafka kTopic = forever $ do
            h <- openFile fp ReadMode
            infoM "Emitting input file"
            messages <- untilM (BS.hGetLine h >>= return . KafkaProduceMessage) (hIsEOF h)
            forM_ (chunksOf emitSizeBatch messages) $ \msgBatch -> do
              errs <- produceMessageBatch kTopic KafkaUnassignedPartition msgBatch
              when ((length errs) > 0) (infoM $ "Error enquining some kafka messages " ++ show errs)
              pollEvents kafka emitSleepMs

            drainOutQueue kafka
