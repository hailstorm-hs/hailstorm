module Hailstorm.Negotiator
( runNegotiator
, spoutStatePipe
) where

import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Maybe
import Pipes
import Hailstorm.Clock
import Hailstorm.MasterState
import Hailstorm.Error
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Topology
import Hailstorm.ZKCluster
import qualified Data.Foldable as Foldable
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK

spoutStatePipe :: ZK.Zookeeper
               -> ProcessorId
               -> MVar MasterState
               -> Pipe (Payload k v) (Payload k v) IO ()
spoutStatePipe zk sid stateMVar = forever $ do
    ms <- lift $ readMVar stateMVar
    case ms of
        GreenLight _ -> passOn
        SpoutPause ->  do
            _ <- lift $ forceEitherIO UnknownWorkerException (setProcessorState zk sid (SpoutPaused "fun" 0))
            lift $ pauseUntilGreen stateMVar
            _ <- lift $ forceEitherIO UnknownWorkerException (setProcessorState zk sid SpoutRunning)
            return ()
        _ -> do
            lift $ putStrLn $ "Spout waiting green light... master state=" ++ show ms
            lift $ threadDelay $ 1000 * 1000 * 10
    where passOn = await >>= yield

pauseUntilGreen :: MVar MasterState -> IO ()
pauseUntilGreen stateMVar = do
    ms <- readMVar stateMVar
    case ms of
        GreenLight _ -> return ()
        _ -> threadDelay (1000 * 1000) >> pauseUntilGreen stateMVar

debugSetMasterState :: ZK.Zookeeper
                    -> MasterState
                    -> IO (Either ZK.ZKError ZK.Stat)
debugSetMasterState zk ms = do
    r <- setMasterState zk ms
    putStrLn $ "Master state set to " ++ show ms
    return r

killFromRef :: IORef (Maybe ThreadId) -> IO ()
killFromRef ioRef = do
    mt <- readIORef ioRef
    Foldable.forM_ mt killThread

waitUntilSnapshotsComplete :: Topology t => ZK.Zookeeper -> t -> IO ()
waitUntilSnapshotsComplete _ _ = return ()

negotiateSnapshot :: (Topology t) => ZK.Zookeeper -> t -> IO Clock
negotiateSnapshot zk t = do
    _ <- forceEitherIO UnknownWorkerException (debugSetMasterState zk SpoutPause)
    offsetsAndPartitions <- untilSpoutsPaused
    return $ Clock (Map.fromList offsetsAndPartitions)

    where untilSpoutsPaused = do
            stateMap <- forceEitherIO UnknownWorkerException (getAllProcessorStates zk)
            let spoutStates = map (\k -> fromJust $ Map.lookup k stateMap) (spoutIds t)
            let spoutsPaused = [(p,o) | (SpoutPaused p o) <- spoutStates]
            if length spoutsPaused == length spoutStates then return spoutsPaused
                else untilSpoutsPaused

runNegotiator :: Topology t => ZKOptions -> t -> IO ()
runNegotiator zkOpts topology = do
    fullChildrenThreadId <- newIORef (Nothing :: Maybe ThreadId)
    registerProcessor zkOpts ("negotiator", 0) UnspecifiedState $ \zk ->
        forceEitherIO
            (DuplicateNegotiatorError
                "Could not set state, probable duplicate process")
            (debugSetMasterState zk Initialization) >> watchLoop zk fullChildrenThreadId
    throw $ ZookeeperConnectionError
        "Negotiator zookeeper registration terminated unexpectedly"
  where
    fullThread zk = forever $ do
        waitUntilSnapshotsComplete zk topology
        threadDelay $ 1000 * 1000 * 5
        nextSnapshotClock <- negotiateSnapshot zk topology
        _ <- forceEitherIO UnknownWorkerException (debugSetMasterState zk (GreenLight nextSnapshotClock))
        return ()

    watchLoop zk fullThreadId = watchProcessors zk $ \childrenEither ->
      case childrenEither of
        Left e -> throw $ wrapInHSError e UnexpectedZookeeperError
        Right children -> do
            killFromRef fullThreadId

            putStrLn $ "Children changed to " ++ show children
            let expectedRegistrations = numProcessors topology + 1

            if length children < expectedRegistrations
              then do
                  putStrLn "Not enough children"
                  _ <- forceEitherIO UnexpectedZookeeperError (debugSetMasterState zk Unavailable)
                  return ()
              else do
                  tid <- forkOS $ fullThread zk
                  writeIORef fullThreadId $ Just tid
