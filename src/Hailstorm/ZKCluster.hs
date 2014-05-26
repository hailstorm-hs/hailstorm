module Hailstorm.ZKCluster
( MasterState(..)
, ZKOptions(..)
, initializeCluster
, watchProcessors
, watchMasterState
, quietZK
, registerProcessor
, setMasterState
, setProcessorState
, getProcessorState
, getAllProcessorStates
, getDebugInfo
) where

import Control.Concurrent
import Control.Monad
import Hailstorm.Clock
import Hailstorm.Processor
import Data.Either
import Data.List.Split
import Data.Map (Map)
import qualified Database.Zookeeper as ZK
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import qualified Data.Map as Map

type ProcessorAction = (ZK.Zookeeper -> IO ())
data ZKOptions       = ZKOptions { connectionString :: String }

data MasterState = Unavailable
                 | Initialization
                 | SpoutPause
                 | GreenLight Clock
                   deriving (Eq, Read, Show)

-- | Master state Zookeeper node.
zkMasterStateNode :: String
zkMasterStateNode = "/master_state"

-- | Zookeeper node for living processors.
zkLivingProcessorsNode :: String
zkLivingProcessorsNode = "/living_processors"

-- | Zookeeper node for a single processor.
zkProcessorNode :: ProcessorId -> String
zkProcessorNode (pname, pinstance) =
    zkLivingProcessorsNode ++ "/" ++ pname ++ "-" ++ show pinstance

-- | Timeout for Zookeeper connections.
zkTimeout :: ZK.Timeout
zkTimeout = 10000

-- | Creates and registers a processor node in Zookeeper.
registerProcessor :: ZKOptions
                  -> ProcessorId
                  -> ProcessorState
                  -> ProcessorAction
                  -> IO ()
registerProcessor opts pid initialState action =
    withConnection opts $ \zk -> do
        me <- ZK.create zk (zkProcessorNode pid)
            (Just $ serializeZK initialState) ZK.OpenAclUnsafe [ZK.Ephemeral]
        case me of
            Left e  -> putStrLn $
                "Error while registering " ++ show pid ++ ": " ++ show e
            Right _ -> do putStrLn $ "Added to zookeeper: " ++ show pid
                          action zk

-- | Initializes Zookeeper cluster for Hailstorm.
initializeCluster :: ZKOptions -> IO ()
initializeCluster opts = withConnection opts $ \zk -> do
    pnode <- ZK.create zk zkLivingProcessorsNode Nothing ZK.OpenAclUnsafe []
    mnode <- ZK.create zk zkMasterStateNode Nothing ZK.OpenAclUnsafe []
    case pnode of
        Left e -> error $ "Error creating living processors node: " ++ show e
        Right _ -> return ()
    case mnode of
        Left e -> error $ "Error creating master state node: " ++ show e
        Right _ -> return ()

-- | Gets debug information from Zookeeper.
getDebugInfo :: ZKOptions -> IO String
getDebugInfo opts = withConnection opts $ \zk -> do
    s1 <- getLivingProcessorsInfo zk
    s2 <- getMasterStateInfo zk
    return $ s1 ++ "\n" ++ s2
  where
    getLivingProcessorsInfo zk = do
        me <- ZK.getChildren zk zkLivingProcessorsNode Nothing
        case me of
            Left e  -> return $ "Could not get living processors: " ++ show e
            Right p -> return $ "Living processors: " ++ show p
    getMasterStateInfo zk = do
        me <- ZK.get zk zkMasterStateNode Nothing
        case me of
            Left e  -> return $ "Could not get master state info: " ++ show e
            Right p -> return $ "Master state: " ++ show p

-- | Delivers living processors change events to the callback. Uses the same
-- thread as was called in with.
watchProcessors :: ZK.Zookeeper
                -> (Either ZK.ZKError [String] -> IO ())
                -> IO ()
watchProcessors zk callback = do
    childrenVar <- newMVar True
    _ <- ZK.getChildren zk zkLivingProcessorsNode (Just $ watcher childrenVar)
    childLoop childrenVar []
  where
    watcher childrenVar _ _ _ _ = putMVar childrenVar True
    childLoop childrenVar lastChildren = do
        _ <- takeMVar childrenVar
        me <- ZK.getChildren zk zkLivingProcessorsNode
            (Just $ watcher childrenVar)
        case me of
            Left e -> callback (Left e) >> childLoop childrenVar lastChildren
            Right children -> do
                when (children /= lastChildren) (callback $ Right children)
                childLoop childrenVar children

-- | Delivers master state change events to the callback. Uses the same thread
-- as was called in with.
watchMasterState :: ZK.Zookeeper
                   -> (Either ZK.ZKError MasterState -> IO ())
                   -> IO ()
watchMasterState zk callback = do
    mVar <- newMVar True
    _ <- ZK.get zk zkMasterStateNode (Just $ watcher mVar)
    watchLoop mVar Unavailable
  where
    watcher mVar _ _ _ _ = putMVar mVar True
    watchLoop mVar lastState = do
        _ <- takeMVar mVar
        me <- ZK.get zk zkMasterStateNode (Just $ watcher mVar)
        case me of
            Left e -> callback (Left e) >> watchLoop mVar lastState
            Right (Just s, _) -> do
                let ms = deserializeZK s :: MasterState
                when (lastState /= ms) (callback $ Right ms)
                watchLoop mVar ms
            _ -> callback (Left ZK.NothingError) >> watchLoop mVar lastState

-- | Sets state of master node.
setMasterState :: ZK.Zookeeper -> MasterState -> IO (Either ZK.ZKError ZK.Stat)
setMasterState zk ms = ZK.set zk zkMasterStateNode
    (Just $ serializeZK ms) Nothing

-- | Sets state of processor in Zookeeper.
setProcessorState :: ZK.Zookeeper
                  -> ProcessorId
                  -> ProcessorState
                  -> IO (Either ZK.ZKError ZK.Stat)
setProcessorState zk pid pstate = ZK.set zk (zkProcessorNode pid)
    (Just $ serializeZK pstate) Nothing

-- | Gets state of processor from Zookeeper.
getProcessorState :: ZK.Zookeeper
                  -> ProcessorId
                  -> IO (Either ZK.ZKError ProcessorState)
getProcessorState zk pid = do
    state' <- ZK.get zk (zkProcessorNode pid) Nothing
    return $ case state' of
        Left e -> Left e
        Right (Just s, _) -> let ps = deserializeZK s :: ProcessorState
                             in Right ps
        _ -> Left ZK.NothingError

-- | Gets state of each living processor in Zookeeper, and returns it in
-- map form.
getAllProcessorStates :: ZK.Zookeeper
                      -> IO (Either ZK.ZKError (Map ProcessorId ProcessorState))
getAllProcessorStates zk = do
    children' <- ZK.getChildren zk zkLivingProcessorsNode Nothing
    case children' of
        Left e -> return $ Left e
        Right children -> do
            let pids = map processorNodeToId children
                processorNodeToId s =
                    case splitOn "-" s of
                        [pname, pinstance] -> (pname, read pinstance)
                        _ -> error $ "Unexpected processor name " ++ s
            states' <- mapM (getProcessorState zk) pids
            return $ case lefts states' of
                         firstErr:_ -> Left firstErr
                         [] -> Right (Map.fromList $ zip pids (rights states'))

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
