{-# LANGUAGE ScopedTypeVariables #-}
module Hailstorm.Processor.Pool
( downstreamPoolConsumer
) where

import Control.Exception
import Data.ByteString.Char8 ()
import Hailstorm.Error
import Hailstorm.Payload
import Hailstorm.Processor
import Hailstorm.Topology
import Network.Simple.TCP
import Network.Socket(socketToHandle)
import Pipes
import System.IO
import qualified Data.Map as Map
import qualified System.Log.Logger as L

infoM :: String -> IO ()
infoM = L.infoM "Hailstorm.Processor.Pool"

type Host = String
type Port = String

-- | @poolConnect address handleMap@ will return a handle for communication
-- with a processor, using an existing handle if one exists in
-- @handleMap@, creating a new connection to the host otherwise.
poolConnect :: (Host, Port) -> Map.Map (Host, Port) Handle -> IO Handle
poolConnect (host, port) handleMap = case Map.lookup (host, port) handleMap of
    Just h -> return h
    Nothing -> connect host port $ \(s, _) -> do
              h <- socketToHandle s ReadWriteMode
              hSetBuffering h LineBuffering
              return h

-- | Produces a single Consumer comprised of all stream consumer layers of
-- the topology (bolts and sinks) that subscribe to a emitting processor's
-- stream. Payloads received by the consumer are sent to the next layer in
-- the topology.
downstreamPoolConsumer :: Topology t
                   => ProcessorName
                   -> t
                   -> Consumer Payload IO ()
downstreamPoolConsumer pName topology = emitToNextLayer Map.empty
  where
    emitToNextLayer connPool = do
        payload <- await
        let sendAddresses = downstreamAddresses topology pName payload
            getHandle addressTuple = lift $ poolConnect addressTuple connPool
            emitToHandle h = lift $ 
                catch (do
                        hPutStrLn h $ serializePayload payload $ serializer pr
                        _ <- hGetLine h -- Wait for ack 
                        return ()
                      )
                      (\(ex :: IOException) -> do
                          infoM $ "Caught exception in " ++ pName ++ ": " ++ show ex
                          throw $ DisconnectionError $ "Disconnected in " ++ pName
                      )


        newHandles <- mapM getHandle sendAddresses
        mapM_ emitToHandle newHandles
        let newPool = Map.fromList $ zip sendAddresses newHandles
        emitToNextLayer $ Map.union newPool connPool
    pr = case lookupProcessor pName topology of
         Just x -> x
         Nothing -> throw $ BadStateError $ "Unable to find " ++ pName ++ " in topology"
