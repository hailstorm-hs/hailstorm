module Hailstorm.Processor.Spout
( runSpout
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
import Data.IORef
import Hailstorm.Clock
import Hailstorm.Concurrency
import Hailstorm.Error
import Hailstorm.InputSource
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Processor.Pool
import Hailstorm.Topology
import Hailstorm.UserFormula
import Hailstorm.ZKCluster
import Hailstorm.ZKCluster.MasterState
import Hailstorm.ZKCluster.ProcessorState
import Pipes
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Processor.Spout"

-- | Start processing with a spout.
runSpout :: (Topology t, InputSource s)
         => ZKOptions
         -> ProcessorName
         -> Partition
         -> t
         -> s
         -> UserFormula k v
         -> IO ()
runSpout zkOpts pName partition topology inputSource uFormula = do
    index <- partitionIndex inputSource partition
    let spoutId = (pName, index)
    registerProcessor zkOpts spoutId SpoutRunning $ \zk -> do
        masterStateMVar <- newEmptyMVar
        tid <- forkOS $ pipeThread zk spoutId masterStateMVar 0
        spoutRunnerIdRef <- newIORef tid

        watchMasterState zk $ \et -> case et of
            Left e -> throw $ HSErrorWrap UnexpectedZookeeperError (show e)
            Right (SpoutsRewind (Clock pMap)) -> do
                oldTid <- readIORef spoutRunnerIdRef
                infoM $ "Rewind detected, killing " ++ show oldTid
                killThread oldTid
                waitForThreadDead oldTid
                let newOffset = pMap Map.! partition
                newTid <- forkOS $ pipeThread zk spoutId masterStateMVar newOffset
                infoM $ "Rewound thread to " ++ show (partition, newOffset)
                writeIORef spoutRunnerIdRef newTid

            Right ms -> signalState masterStateMVar ms

    throw $ ZookeeperConnectionError $ "Unable to register spout " ++ pName
  where
    signalState mVar ms = tryTakeMVar mVar >> putMVar mVar ms 
    pipeThread zk spoutId stateMVar offset =
      let downstream = downstreamPoolConsumer pName topology uFormula
          producer = partitionProducer inputSource partition offset
      in runEffect $
        producer >-> spoutStatePipe zk spoutId partition offset uFormula stateMVar >-> downstream

spoutStatePipe :: ZK.Zookeeper
               -> ProcessorId
               -> Partition
               -> Offset
               -> UserFormula k v
               -> MVar MasterState
               -> Pipe InputTuple (Payload k v) IO ()
spoutStatePipe zk spoutId partition lastOffset uFormula stateMVar = do
    ms <- lift $ readMVar stateMVar
    case ms of
        Flowing _ -> passOn
        _ ->  do
            void <$> lift $ forceEitherIO UnknownWorkerException
                (setProcessorState zk spoutId $ SpoutPaused partition lastOffset)
            lift $ pauseUntilFlowing stateMVar
            void <$> lift $ forceEitherIO UnknownWorkerException
                (setProcessorState zk spoutId SpoutRunning)
            loop

  where
    passOn = do
        InputTuple bs p o <- await
        yield Payload { payloadTuple = convertFn uFormula bs
                      , payloadPosition = (p, o)
                      , payloadLowWaterMarkMap = Map.singleton partition $
                          Clock $ Map.singleton p o
                      }
        spoutStatePipe zk spoutId partition o uFormula stateMVar
    loop = spoutStatePipe zk spoutId partition lastOffset uFormula stateMVar

pauseUntilFlowing :: MVar MasterState -> IO ()
pauseUntilFlowing stateMVar = do
    ms <- readMVar stateMVar
    case ms of
        Flowing _ -> return ()
        _ -> threadDelay (1000 * 1000) >> pauseUntilFlowing stateMVar

