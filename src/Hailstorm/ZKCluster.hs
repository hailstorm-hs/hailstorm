module Hailstorm.ZKCluster
( ZKOptions (..)
, registerProcessor
, initializeCluster
, getStatus
, childrenWatchLoop
) where

import Control.Concurrent
import Control.Monad
import qualified Database.Zookeeper as ZK

type ProcessorId     = String
type ProcessorAction = (ZK.Zookeeper -> IO ())
data ZKOptions       = ZKOptions { connectionString :: String }

data MasterState = Unavailable | SpoutPause | GreenLight
    deriving (Eq, Read, Show)

-- | Zookeeper node for living processors.
zkLivingProcessorsNode :: String
zkLivingProcessorsNode = "/living_processors"

-- | Master state 
zkMasterStateNode :: String
zkMasterStateNode = "/master_state"

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

childrenWatchLoop :: ZK.Zookeeper -> String -> ([String] -> IO ()) -> IO ()
childrenWatchLoop zk path cb = do
    childrenVar <- newMVar True
    _ <- ZK.getChildren zk path (Just $ watcher childrenVar)
    childLoop childrenVar

    where watcher childrenVar _ _ _ _ = do
            putStrLn $ "Watcher saw a node change"
            putMVar childrenVar True

          childLoop childrenVar = forever $ do
            _ <- takeMVar childrenVar
            me <- ZK.getChildren zk path (Just $ watcher childrenVar)
            case me of
              Left e -> putStrLn $ "Error in children watch loop from zookeeper: " ++ (show e)
              Right children -> cb children

createMasterState :: ZK.Zookeeper -> IO (Either ZK.ZKError String)
createMasterState zk = ZK.create zk zkMasterStateNode Nothing ZK.OpenAclUnsafe [ZK.Ephemeral]

-- @withConnection opts action@ runs @action@, which takes a Zookeeper handler.
withConnection :: ZKOptions -> (ZK.Zookeeper -> IO a) -> IO a
withConnection opts = ZK.withZookeeper (connectionString opts) zkTimeout
    Nothing Nothing
