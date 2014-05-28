module Hailstorm.ZKCluster
( ZKOptions(..)
, withConnection
, serializeZK
, deserializeZK
, quietZK
, zkThrottle
) where

import Control.Concurrent

import qualified Data.ByteString.Char8 as C8
import qualified Database.Zookeeper as ZK
import qualified Data.ByteString as BS

data ZKOptions = ZKOptions { connectionString :: String }

-- | Timeout for Zookeeper connections.
zkTimeout :: ZK.Timeout
zkTimeout = 10000

-- | Friendly amount of time to wait while pinging ZK
zkPingInterval :: Int
zkPingInterval = 1000 * 1000

-- | Friendly throttling delay between ZK pings
zkThrottle :: IO ()
zkThrottle = threadDelay zkPingInterval

-- | Reduces output level of Zookeeper to warnings-only.
quietZK :: IO ()
quietZK = ZK.setDebugLevel ZK.ZLogWarn

-- | Deserializes element from storage in Zookeeper.
deserializeZK :: Read t => BS.ByteString -> t
deserializeZK = read . C8.unpack

-- | Seriealizes element for storage in Zookeeper.
serializeZK :: Show t => t -> BS.ByteString
serializeZK = C8.pack . show

-- | @withConnection opts action@ runs @action@, which takes a Zookeeper
-- handler.
withConnection :: ZKOptions -> (ZK.Zookeeper -> IO a) -> IO a
withConnection opts = ZK.withZookeeper (connectionString opts) zkTimeout
    Nothing Nothing
