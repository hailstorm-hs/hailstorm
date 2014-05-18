module Hailstorm.ZKCluster
( ZKOptions (..)
, registerProcessor
, initializeCluster
, getStatus
) where

import qualified Database.Zookeeper as ZK

type ProcessorId     = String
type ProcessorAction = (ZK.Zookeeper -> IO ())
data ZKOptions       = ZKOptions { connectionString :: String }

-- | Zookeeper node for living processors.
zkLivingProcessorsNode :: String
zkLivingProcessorsNode = "/living_processors"

-- | Timeout for Zookeeper connections.
zkTimeout :: ZK.Timeout
zkTimeout = 10000

registerProcessor :: ZKOptions
                  -> ProcessorId
                  -> ProcessorAction
                  -> IO ()
registerProcessor opts processorId action =
    withConnection opts $ \zk -> do
        me <- ZK.create zk (zkLivingProcessorsNode ++ "/" ++ processorId)
            Nothing ZK.OpenAclUnsafe [ZK.Ephemeral]
        case me of
            Left e  -> putStrLn $ "Error from zookeeper: "  ++ show e
            Right _ -> do putStrLn $ "Added to zookeeper: " ++ processorId
                          action zk

initializeCluster :: ZKOptions -> IO ()
initializeCluster opts = withConnection opts $ \zk -> do
    me <- ZK.create zk zkLivingProcessorsNode Nothing ZK.OpenAclUnsafe []
    case me of
        Left e -> putStrLn $ "Error from zookeeper: " ++ show e
        Right _ -> return ()

getStatus :: ZKOptions -> IO String
getStatus opts = withConnection opts $ \zk -> do
    me <- ZK.getChildren zk zkLivingProcessorsNode Nothing
    case me of
        Left e  -> return $ "Error from zookeeper: " ++ show e
        Right p -> return $ "Living processors: " ++ show p

-- @withConnection opts action@ runs @action@, which takes a Zookeeper handler.
withConnection :: ZKOptions -> (ZK.Zookeeper -> IO a) -> IO a
withConnection opts = ZK.withZookeeper (connectionString opts) zkTimeout
    Nothing Nothing
