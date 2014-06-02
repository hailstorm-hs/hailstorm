module Hailstorm.Processor.Spout
( runSpout
) where

import Control.Concurrent hiding (yield)
import Control.Exception
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
         -> Spout
         -> ProcessorInstance
         -> Partition
         -> t
         -> s
         -> IO ()
runSpout zkOpts sp instNum partition topology inputSource = do
    let spoutId = (spoutName sp, instNum)

    groundhogDay (runSpout zkOpts sp instNum partition topology inputSource) $
        registerProcessor zkOpts spoutId SpoutRunning $ \zk -> do
            masterStateMVar <- newEmptyMVar
            tid <- forkOS $ pipeThread zk masterStateMVar 0
            spoutRunnerIdRef <- newIORef tid

            watchMasterState zk $ \et -> case et of
                Left e -> throw $ HSErrorWrap UnexpectedZookeeperError (show e)
                Right (SpoutsRewind (Clock pMap)) -> do
                    oldTid <- readIORef spoutRunnerIdRef
                    infoM $ "Rewind detected, killing " ++ show oldTid
                    killThread oldTid
                    waitForThreadDead oldTid
                    let newOffset = pMap Map.! partition
                    newTid <- forkOS $ pipeThread zk masterStateMVar newOffset
                    infoM $ "Rewound thread to " ++ show (partition, newOffset)
                    writeIORef spoutRunnerIdRef newTid

                Right ms -> signalState masterStateMVar ms

    throw $ ZookeeperConnectionError $ "Unable to register spout " ++ show spoutId
  where
    signalState mVar ms = tryTakeMVar mVar >> putMVar mVar ms
    pipeThread zk stateMVar offset =
      let downstream = downstreamPoolConsumer (spoutName sp) topology
          producer = partitionProducer inputSource partition offset
      in runEffect $
        producer >-> spoutStatePipe zk sp instNum partition offset stateMVar >-> downstream

spoutStatePipe :: ZK.Zookeeper
               -> Spout
               -> ProcessorInstance
               -> Partition
               -> Offset
               -> MVar MasterState
               -> Pipe InputTuple Payload IO ()
spoutStatePipe zk sp instNum partition lastOffset stateMVar = do
    let spoutId = (spoutName sp, instNum)
    ms <- lift $ readMVar stateMVar
    case ms of
        Flowing _ -> passOn
        _ ->  do
            lift $ forceSetProcessorState zk spoutId $
                SpoutPaused partition lastOffset
            lift $ pauseUntilFlowing stateMVar
            lift $ forceSetProcessorState zk spoutId SpoutRunning
            loop

  where
    passOn = do
        InputTuple bs p o <- await
        yield Payload { payloadTuple = convertFn sp bs
                      , payloadPosition = (p, o)
                      , payloadLowWaterMarkMap = Map.singleton partition $
                          Clock $ Map.singleton p o
                      }
        spoutStatePipe zk sp instNum partition o stateMVar
    loop = spoutStatePipe zk sp instNum partition lastOffset stateMVar

pauseUntilFlowing :: MVar MasterState -> IO ()
pauseUntilFlowing stateMVar = do
    ms <- readMVar stateMVar
    case ms of
        Flowing _ -> return ()
        _ -> threadDelay (1000 * 1000) >> pauseUntilFlowing stateMVar

