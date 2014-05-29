module Hailstorm.InputSource.KafkaSource
( KafkaOptions(..)
, kafkaFromOptions
) where

import Haskakafka
import Hailstorm.Error

data KafkaOptions = KafkaOptions { brokerConnectionString :: String, topic :: String }

kafkaFromOptions :: KafkaOptions -> KafkaType -> IO (Either HSError (Kafka, KafkaTopic))
kafkaFromOptions kOpts t = do
    kConf <- newKafkaConf
    kTopicConf <- newKafkaTopicConf

    kafka <- newKafka t kConf
    addBrokers kafka $ brokerConnectionString kOpts
    kTopic <- newKafkaTopic kafka (topic kOpts) kTopicConf

    return $ Right (kafka, kTopic)
