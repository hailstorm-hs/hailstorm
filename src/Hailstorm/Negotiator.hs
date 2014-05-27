module Hailstorm.Negotiator
( runNegotiator
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Maybe
import Hailstorm.Clock
import Hailstorm.MasterState
import Hailstorm.Error
import Hailstorm.Processor
import Hailstorm.Topology
import Hailstorm.ZKCluster
import qualified Data.Foldable as Foldable
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK

runNegotiator :: Topology t => ZKOptions -> t -> IO ()
runNegotiator zkOpts topology = do
    fullChildrenThreadId <- newIORef (Nothing :: Maybe ThreadId)
    registerProcessor zkOpts ("negotiator", 0) UnspecifiedState $ \zk ->
        forceEitherIO
            (DuplicateNegotiatorError "Could not set state: duplicate process?")
            (createMasterState zk Initialization) >>
                watchLoop zk fullChildrenThreadId
    throw $ ZookeeperConnectionError "Unable to register Negotiator"
  where
    fullThread zk = forever $ do
        waitUntilSnapshotsComplete zk topology
        threadDelay $ 1000 * 1000 * 5
        nextSnapshotClock <- negotiateSnapshot zk topology
        void <$> forceEitherIO UnknownWorkerException $
            debugSetMasterState zk $ Flowing (Just nextSnapshotClock)

    watchLoop zk fullThreadId = watchProcessors zk $ \childrenEither ->
        case childrenEither of
            Left e -> throw $ wrapInHSError e UnexpectedZookeeperError
            Right children -> do
                killFromRef fullThreadId

                putStrLn $ "Processors changed: " ++ show children
                let expectedRegistrations = numProcessors topology + 1

                if length children < expectedRegistrations
                    then do
                        putStrLn "Not enough children"
                        void <$> forceEitherIO UnexpectedZookeeperError $
                            debugSetMasterState zk Unavailable
                    else do
                        tid <- forkOS $ fullThread zk
                        writeIORef fullThreadId $ Just tid

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
    void <$> forceEitherIO UnknownWorkerException $
        debugSetMasterState zk Blocked
    partitionsAndOffsets <- untilSpoutsPaused
    return $ Clock (Map.fromList partitionsAndOffsets)

    where untilSpoutsPaused = do
            stateMap <- forceEitherIO UnknownWorkerException $
                getAllProcessorStates zk
            let spoutStates = map (\k -> stateMap Map.! k ) (spoutIds t)
                spoutsPaused = [(p,o) | (SpoutPaused p o) <- spoutStates]

            if length spoutsPaused == length spoutStates
                then return spoutsPaused
                else untilSpoutsPaused
