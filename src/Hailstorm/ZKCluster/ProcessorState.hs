{-# LANGUAGE ScopedTypeVariables #-}
module Hailstorm.ZKCluster.ProcessorState
( ProcessorState(..)
, getDebugInfo
, groundhogDay
, initializeCluster
, registerProcessor
, forceSetProcessorState
, getAllProcessorStates
, watchProcessors
, zkLivingProcessorsNode
) where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import Hailstorm.Error
import Hailstorm.Processor
import Data.Either
import Data.List.Split
import Data.Map (Map)
import Hailstorm.Clock
import Hailstorm.ZKCluster
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.ZKCluster.ProcessorState"

errorM :: String -> IO ()
errorM = L.errorM "Hailstorm.ZKCluster.ProcessorState"

data ProcessorState = BoltRunning
                    | SinkRunning
                    | SpoutPaused Partition Offset
                    | SpoutRunning
                    | BoltSaved Clock
                    | BoltLoaded Clock
                    | UnspecifiedState
                      deriving (Eq, Show, Read)

type ProcessorAction = (ZK.Zookeeper -> IO ())

-- | Zookeeper node for living processors.
zkLivingProcessorsNode :: String
zkLivingProcessorsNode = "/living_processors"

-- | Zookeeper node for a single processor.
zkProcessorNode :: ProcessorId -> String
zkProcessorNode (pname, pinstance) =
    zkLivingProcessorsNode ++ "/" ++ pname ++ "-" ++ show pinstance

-- | Gets debug information from Zookeeper.
getDebugInfo :: ZKOptions -> IO String
getDebugInfo opts = withConnection opts $ \zk -> getLivingProcessorsInfo zk
  where
    getLivingProcessorsInfo zk = do
        me <- ZK.getChildren zk zkLivingProcessorsNode Nothing
        case me of
            Left e  -> return $ "Could not get living processors: " ++ show e
            Right p -> return $ "Living processors: " ++ show p

-- | Initializes Zookeeper cluster for Hailstorm.
initializeCluster :: ZKOptions -> IO ()
initializeCluster opts = withConnection opts $ \zk -> do
    pnode <- ZK.create zk zkLivingProcessorsNode Nothing ZK.OpenAclUnsafe []
    case pnode of
        Left e -> errorM $
            "Could not create living processors node: " ++ show e
        Right _ -> return ()

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
            Left e  -> errorM $
                "Error while registering " ++ show pid ++ ": " ++ show e
            Right _ -> do
                infoM $ "Registered: " ++ show pid
                addDeletionWatcher zk pid
                action zk

processorRestartDelay :: Int
processorRestartDelay = 15 * 1000 * 1000

-- | Catches registration deleted events, restarts process
groundhogDay :: IO () -> IO () -> IO ()
groundhogDay handler action =
    catch action (\(_ :: SomeException) -> threadDelay processorRestartDelay >> handler)

-- | Creates a watcher that throws to the master thread whenever
-- the registration is deleted from Zookeeper by the negotiator
addDeletionWatcher :: ZK.Zookeeper -> ProcessorId -> IO ()
addDeletionWatcher zk pid = do
    masterTid <- myThreadId
    void <$> forkOS $ do
        me <- ZK.get zk (zkProcessorNode pid) (Just $ watcher masterTid)
        case me of
            Left e -> doubleThrow masterTid (ZookeeperConnectionError $ show e)
            Right _ -> return ()
  where
    watcher masterTid _ ZK.DeletedEvent _ _ =
        throwTo masterTid RegistrationDeleted
    watcher _ _ _ _ _ = return ()


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

-- | Set processor state, but force IO Exception on failure.
forceSetProcessorState :: ZK.Zookeeper
                       -> ProcessorId
                       -> ProcessorState
                       -> IO ()
forceSetProcessorState zk pId pState = void <$>
    forceEitherIO UnknownWorkerException $ setProcessorState zk pId pState

-- | Sets state of processor in Zookeeper.
setProcessorState :: ZK.Zookeeper
                  -> ProcessorId
                  -> ProcessorState
                  -> IO (Either ZK.ZKError ZK.Stat)
setProcessorState zk pid pState = ZK.set zk (zkProcessorNode pid)
    (Just $ serializeZK pState) Nothing

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
