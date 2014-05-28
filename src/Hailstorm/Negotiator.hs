module Hailstorm.Negotiator
( runNegotiator
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.IORef
import Hailstorm.Clock
import Hailstorm.InputSource
import Hailstorm.Error
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
        void <$> forceEitherIO UnknownWorkerException $ debugSetMasterState zk Initialization
        clocks <- untilBoltsLoaded zk topology
        unless (allTheSame clocks) (doubleThrow masterThreadId 
            (BadStartupError $ "Bolts started at different points " ++ show clocks))

        clock <- startClock inputSource -- TODO: use above clocks
        void <$> forceEitherIO UnknownWorkerException $ debugSetMasterState zk $ SpoutsRewind clock
        _ <- untilSpoutsPaused zk topology
        flowLoop zk

    flowLoop zk = forever $ do
        infoM "Allowing grace period before next snapshot..."
        void <$> forceEitherIO UnknownWorkerException $ debugSetMasterState zk $ Flowing Nothing
        
        snapshotThrottle

        infoM "Negotiating next snapshot"
        nextSnapshotClock <- negotiateSnapshot zk topology
        void <$> forceEitherIO UnknownWorkerException $
            debugSetMasterState zk $ Flowing (Just nextSnapshotClock)

        infoM "Waiting for bolts to save"
        _ <- untilBoltsSaved zk topology
        return ()

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
                        void <$> forceEitherIO UnexpectedZookeeperError $
                            debugSetMasterState zk Unavailable
                    else do
                        tid <- myThreadId >>= \mtid -> forkOS $ fullChildrenThread zk mtid
                        writeIORef fullChildrenThreadId $ Just tid

debugSetMasterState :: ZK.Zookeeper
                    -> MasterState
                    -> IO (Either ZK.ZKError ZK.Stat)
debugSetMasterState zk ms = do
    r <- setMasterState zk ms
    infoM $ "Master state set to " ++ show ms
    return r

killFromRef :: IORef (Maybe ThreadId) -> IO ()
killFromRef ioRef = do
    mt <- readIORef ioRef
    Foldable.forM_ mt killThread

negotiateSnapshot :: (Topology t) => ZK.Zookeeper -> t -> IO Clock
negotiateSnapshot zk t = do
    void <$> forceEitherIO UnknownWorkerException $
        debugSetMasterState zk SpoutsPaused
    partitionsAndOffsets <- untilSpoutsPaused zk t
    return $ Clock $ Map.fromList partitionsAndOffsets

allTheSame :: (Eq a) => [a] -> Bool
allTheSame [] = True
allTheSame xs = all (== head xs) (tail xs)

untilBoltsLoaded :: Topology t => ZK.Zookeeper -> t -> IO [Clock]
untilBoltsLoaded zk t = do
    stateMap <- forceEitherIO UnknownWorkerException $
        getAllProcessorStates zk
    let boltStates = map (\k -> stateMap Map.! k ) (boltIds t)
        boltsLoaded= [clock | (BoltLoaded clock) <- boltStates]

    if length boltStates /= length boltsLoaded -- TODO: make equality once milind is done
        then return boltsLoaded
        else do
            infoM $ "Waiting for bolts to load: " ++ show boltStates
            zkThrottle >> untilBoltsLoaded zk t

untilBoltsSaved :: Topology t => ZK.Zookeeper -> t -> IO Clock
untilBoltsSaved zk t = do
    stateMap <- forceEitherIO UnknownWorkerException $
        getAllProcessorStates zk
    let boltStates = map (\k -> stateMap Map.! k ) (boltIds t)
        boltsSaved = [clock | (BoltSaved clock) <- boltStates]

    unless (allTheSame boltsSaved) 
        (throw $ BadClusterStateError $ "Cluster bolts saved snapshots at different points " ++ show boltsSaved)
    
    if length boltStates == length boltsSaved
        then return (head boltsSaved)
        else do
            infoM $ "Waiting for bolts to save: " ++ show boltStates
            zkThrottle >> untilBoltsSaved zk t
    
untilSpoutsPaused :: Topology t => ZK.Zookeeper -> t -> IO [(Partition,Offset)]
untilSpoutsPaused zk t = do
    stateMap <- forceEitherIO UnknownWorkerException $
        getAllProcessorStates zk
    let spoutStates = map (\k -> stateMap Map.! k ) (spoutIds t)
        spoutsPaused = [(p,o) | (SpoutPaused p o) <- spoutStates]

    if length spoutsPaused == length spoutStates
        then return spoutsPaused
        else do
            infoM $ "Waiting for spouts to pause: " ++ show spoutStates
            zkThrottle >> untilSpoutsPaused zk t
