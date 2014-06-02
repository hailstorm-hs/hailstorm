module Hailstorm.Sample.WordCountKafkaEmitter 
    ( emitLinesForever
    ) where

import Control.Monad
import Control.Monad.Loops
import Hailstorm.InputSource.KafkaSource
import Haskakafka
import System.IO

import qualified System.Log.Logger as L
import qualified Data.ByteString.Char8 as BS

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Sample/WordCountKafkaEmitter"

emitLinesForever :: FilePath -> KafkaOptions -> Int -> IO ()
emitLinesForever fp kOpts emitSleepMs = do
    infoM "Constructing kafka"
    (Right (kafka, kTopic)) <- kafkaFromOptions kOpts KafkaProducer
    infoM "Constructed kafka, beginning emit loop"

    emitLoop kafka kTopic 

    where 
        emitLoop kafka kTopic = forever $ do
            h <- openFile fp ReadMode
            infoM "Emitting input file"
            untilM_ (do
                    line <- BS.hGetLine h
                    merr <- produceMessage kTopic KafkaUnassignedPartition $ KafkaProduceMessage line

                    case merr of 
                        Just (KafkaResponseError RdKafkaRespErrPartitionEof) -> return ()
                        Just e -> infoM $ "Error enqueing kafka message " ++ show e
                        Nothing -> return ()
                    pollEvents kafka emitSleepMs
                ) (hIsEOF h)

            drainOutQueue kafka
