module Hailstorm.Processor.Spout
( runSpout
) where

import Control.Applicative
import Control.Concurrent hiding (yield)
import Control.Exception
import Control.Monad
import Data.ByteString.Char8 ()
import Hailstorm.Clock
import Hailstorm.Error
import Hailstorm.InputSource
import Hailstorm.MasterState
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Processor.Pool
import Hailstorm.Topology
import Hailstorm.UserFormula
import Hailstorm.ZKCluster
import Pipes
import qualified Data.Map as Map
import qualified Database.Zookeeper as ZK

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
    registerProcessor zkOpts spoutId SpoutRunning $ \zk ->
        injectMasterState zk (pipeThread zk spoutId)
    throw $ ZookeeperConnectionError $ "Unable to register spout " ++ pName
  where
    pipeThread zk spoutId stateMVar =
      let downstream = downstreamPoolConsumer pName topology uFormula
          producer = partitionProducer inputSource partition 0
      in runEffect $
        producer >-> spoutStatePipe zk spoutId partition 0 uFormula stateMVar >-> downstream

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
        Blocked ->  do
            void <$> lift $ forceEitherIO UnknownWorkerException
                (setProcessorState zk spoutId $ SpoutPaused partition lastOffset)
            lift $ pauseUntilFlowing stateMVar
            void <$> lift $ forceEitherIO UnknownWorkerException
                (setProcessorState zk spoutId SpoutRunning)
            loop
        _ -> do
            lift $ putStrLn $
                "Spout waiting for open valve (state: " ++ show ms ++ ")"
            lift $ threadDelay $ 1000 * 1000 * 10
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

