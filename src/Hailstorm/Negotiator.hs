module Hailstorm.Negotiator
( runNegotiator
) where

import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.IORef
import Hailstorm.Clock
import Hailstorm.InputSource
import Hailstorm.Error
import Hailstorm.Processor
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.MasterState
import Hailstorm.ZKCluster.ProcessorState
import Hailstorm.Topology
import qualified Data.Foldable as Foldable
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Negotiator"

snapshotInterval :: Int
snapshotInterval = 10 * 1000 * 1000

snapshotThrottle :: IO ()
snapshotThrottle = threadDelay snapshotInterval

runNegotiator :: (Topology t, InputSource s) => ZKOptions -> t -> s -> IO ()
runNegotiator zkOpts topology inputSource = do
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

        clock <- startClock inputSource -- TODO: use above clocks
        forceSetMasterState zk $ SpoutsRewind clock
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
                killFromRef fullChildrenThreadId

                infoM $ "Processors changed: " ++ show children
                let expectedRegistrations = numProcessors topology + 1

                if length children < expectedRegistrations
                    then do
                        infoM "Waiting for enough children"
                        forceSetMasterState zk Unavailable
                    else do
                        tid <- myThreadId >>= \mtid -> forkOS $ fullChildrenThread zk mtid
                        writeIORef fullChildrenThreadId $ Just tid

killFromRef :: IORef (Maybe ThreadId) -> IO ()
killFromRef ioRef = do
    mt <- readIORef ioRef
    Foldable.forM_ mt killThread

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
    untilStatesMatch "Waiting for bolts to load : " zk (boltIds t)
        (\pStates -> [clock | (BoltLoaded clock) <- pStates])

untilBoltsSaved :: Topology t => ZK.Zookeeper -> t -> Clock -> IO Clock
untilBoltsSaved zk t clock = do
    saveClocks <- untilStatesMatch "Waiting for bolts to save: " zk (boltIds t)
        (\pStates -> [clock | (BoltSaved c) <- pStates, clock == c ])
    return $ head saveClocks

untilSpoutsPaused :: Topology t => ZK.Zookeeper -> t -> IO [(Partition,Offset)]
untilSpoutsPaused zk t =
    untilStatesMatch "Waiting for spouts to pause: " zk (spoutIds t)
        (\pStates -> [(p,o) | (SpoutPaused p o) <- pStates])

untilStatesMatch :: String -> ZK.Zookeeper -> [ProcessorId] -> ([ProcessorState] -> [a]) -> IO [a]
untilStatesMatch infoString zk pids matcher = do
    stateMap <- forceEitherIO UnknownWorkerException $ getAllProcessorStates zk
    let pStates = map (\k -> stateMap Map.! k ) pids
        matched = matcher pStates

    if length pStates == length matched then return matched
    else do
        infoM $ infoString ++ show pStates
        zkThrottle >> untilStatesMatch infoString zk pids matcher

