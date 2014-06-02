module Hailstorm.InputSource.KafkaSource
( KafkaOptions(..)
, KafkaSource(..)
, kafkaFromOptions
) where

import Control.Monad
import Control.Exception
import Haskakafka
import Hailstorm.Clock
import Hailstorm.InputSource
import Hailstorm.Error
import Pipes

import qualified Data.Map as Map
import qualified System.Log.Logger as L

errorM :: String -> IO ()
errorM = L.errorM "Hailstorm.InputSource.KafkaSource"

kafkaTimeout :: Int
kafkaTimeout = 1000 * 10

data KafkaOptions = KafkaOptions 
  { brokerConnectionString :: String
  , topic :: String 
  } deriving (Eq, Show)

kafkaFromOptions :: KafkaOptions -> KafkaType -> IO (Either HSError (Kafka, KafkaTopic))
kafkaFromOptions kOpts t = do
    kConf <- newKafkaConf
    kTopicConf <- newKafkaTopicConf

    kafka <- newKafka t kConf
    addBrokers kafka $ brokerConnectionString kOpts
    kTopic <- newKafkaTopic kafka (topic kOpts) kTopicConf

    return $ Right (kafka, kTopic)

data KafkaSource = KafkaSource 
  { kafkaOptions :: KafkaOptions
  } deriving (Eq, Show)

instance InputSource KafkaSource where
  partitionProducer (KafkaSource kOpts) partitionStr offset = do
    let partition = read partitionStr :: Int
    (_, kTopic) <- lift $ forceEitherIO UnexpectedKafkaError $ kafkaFromOptions kOpts KafkaConsumer
    lift $ startConsuming kTopic partition $ KafkaOffset (fromIntegral offset)
    kConsumer kTopic partition

    where
      kConsumer kTopic partition = forever $ do
        me <- lift $ consumeMessage kTopic partition kafkaTimeout
        case me of 
          Left e -> lift $ errorM $ "Got error while consuming from Kafka: " ++ show e
          Right m -> 
            yield $ InputTuple (messagePayload m) 
                               (show $ messagePartition m) 
                               (fromIntegral $ messageOffset m)

  allPartitions (KafkaSource kOpts) = do
    (kafka, kTopic) <- forceEitherIO UnexpectedKafkaError $ kafkaFromOptions kOpts KafkaConsumer
    md <- forceEitherIO UnexpectedKafkaError $ getTopicMetadata kafka kTopic kafkaTimeout
    forM (topicPartitions md) $ \et -> case et of
        Left _ -> throw UnexpectedKafkaError
        Right tmd -> return $ show $ partitionId tmd

  startClock s = allPartitions s >>= \ps ->
    return $ Clock $ Map.fromList $ zip ps (repeat (- 2))
