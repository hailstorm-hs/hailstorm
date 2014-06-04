module Hailstorm.Negotiator
( runNegotiator
) where

import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Maybe
import Hailstorm.Clock
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.Processor
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.MasterState
import Hailstorm.ZKCluster.ProcessorState
import Hailstorm.Topology
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Negotiator"

snapshotInterval :: Int
snapshotInterval = 10 * 1000 * 1000

snapshotThrottle :: IO ()
snapshotThrottle = threadDelay snapshotInterval

runNegotiator :: (Topology t) => ZKOptions -> t -> IO ()
runNegotiator zkOpts topology = do
    fullChildrenThreadId <- newIORef (Nothing :: Maybe ThreadId)
    registerProcessor zkOpts ("negotiator", 0) UnspecifiedState $ \zk ->
        forceEitherIO
            (DuplicateNegotiatorError "Could not set state: duplicate process?")
            (createMasterState zk Initialization) >>
                watchLoop zk fullChildrenThreadId
    throw $ ZookeeperConnectionError "Unable to register Negotiator"
  where
    fullChildrenThread zk masterThreadId = do
        forceSetMasterState zk Initialization
        clocks <- untilBoltsLoaded zk topology
        unless (allTheSame clocks) (doubleThrow masterThreadId
            (BadStartupError $ "Bolts started at different points " ++ show clocks))

        forceSetMasterState zk $ SpoutsRewind (head clocks)
        _ <- untilSpoutsPaused zk topology
        flowLoop zk

    flowLoop zk = forever $ do
        infoM "Allowing grace period before next snapshot..."
        forceSetMasterState zk $ Flowing Nothing

        snapshotThrottle

        infoM "Negotiating next snapshot"
        nextSnapshotClock <- negotiateSnapshot zk topology
        forceSetMasterState zk $ Flowing (Just nextSnapshotClock)

        infoM $ "Waiting for bolts to save to negotiated " ++ show nextSnapshotClock
        clock <- untilBoltsSaved zk topology nextSnapshotClock
        infoM $ "Bolts saved at " ++ show clock

    watchLoop zk fullChildrenThreadId = watchProcessors zk $ \childrenEither ->
        case childrenEither of
            Left e -> throw $ wrapInHSError e UnexpectedZookeeperError
            Right children -> do
                killFromRef zk fullChildrenThreadId children

                let expectedRegistrations = numProcessors topology + 1
                infoM $ "Processors changed: (expected=" ++ show expectedRegistrations
                        ++ ",got=" ++ (show $ length children) ++ ") "++ show children

                if length children < expectedRegistrations
                    then do
                        infoM "Waiting for enough children"
                        forceSetMasterState zk Unavailable
                    else do
                        tid <- myThreadId >>= \mtid -> forkOS $ fullChildrenThread zk mtid
                        writeIORef fullChildrenThreadId $ Just tid

-- | Kill all other nodes on a single node death (if we had a full thread)
killFromRef :: ZK.Zookeeper -> IORef (Maybe ThreadId) -> [String] -> IO ()
killFromRef zk ioRef children = do
    mt <- readIORef ioRef
    case mt of
        Just tid -> do
            killThread tid
            writeIORef ioRef Nothing
            mapM_ (\child ->
                    unless (child == "negotiator-0") $ do
                        infoM $ "Negotiator is rebooting processor " ++ child
                        _ <- ZK.delete zk (zkLivingProcessorsNode ++ "/" ++ child) Nothing
                        return ()
                   ) children
        Nothing -> return ()

negotiateSnapshot :: (Topology t) => ZK.Zookeeper -> t -> IO Clock
negotiateSnapshot zk t = do
    forceSetMasterState zk SpoutsPaused
    partitionsAndOffsets <- untilSpoutsPaused zk t
    return $ Clock $ Map.fromList partitionsAndOffsets

allTheSame :: (Eq a) => [a] -> Bool
allTheSame [] = True
allTheSame xs = all (== head xs) (tail xs)

untilBoltsLoaded :: Topology t => ZK.Zookeeper -> t -> IO [Clock]
untilBoltsLoaded zk t =
    untilStatesMatch "Waiting for bolts to load : " zk (bolts t)
        (\pStates -> [clock | (BoltLoaded clock) <- pStates])

untilBoltsSaved :: Topology t => ZK.Zookeeper -> t -> Clock -> IO Clock
untilBoltsSaved zk t clock = do
    saveClocks <- untilStatesMatch "Waiting for bolts to save: " zk (bolts t)
        (\pStates -> [clock | (BoltSaved c) <- pStates, clock == c ])
    return $ head saveClocks

untilSpoutsPaused :: Topology t => ZK.Zookeeper -> t -> IO [(Partition, Offset)]
untilSpoutsPaused zk t =
    untilStatesMatch "Waiting for spouts to pause: " zk (spouts t)
        (\pStates -> [(p,o) | (SpoutPaused p o) <- pStates])

untilStatesMatch :: (Processor p)
                 => String
                 -> ZK.Zookeeper
                 -> [p]
                 -> ([ProcessorState] -> [a])
                 -> IO [a]
untilStatesMatch infoString zk ps matcher = do
    stateMap <- forceEitherIO UnknownWorkerException $ getAllProcessorStates zk
    let pids = concatMap processorIds ps
        pStates = map (\k -> fromMaybe
            (error $ "Could not find state " ++ show k ++ " in states " ++ show (Map.keys stateMap)) $
            Map.lookup k stateMap) pids
        matched = matcher pStates

    if length pStates == length matched
        then return matched
        else do
            infoM $ infoString ++ show pStates
            zkThrottle >> untilStatesMatch infoString zk ps matcher
