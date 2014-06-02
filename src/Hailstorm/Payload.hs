module Hailstorm.Payload
( Payload(..)
, serializePayload
, deserializePayload
) where

import Data.List
import Data.List.Split
import Hailstorm.Clock
import Hailstorm.Processor
import Hailstorm.TransactionTypes
import qualified Data.Map as Map

data Payload = Payload
    { payloadTuple :: PayloadTuple
    , payloadPosition :: (Partition, Offset)
    , payloadLowWaterMarkMap :: Map.Map ProcessorName Clock
    }

serializePayload :: Payload
                 -> (PayloadTuple -> String)
                 -> String
serializePayload payload serializeFn =
    intercalate "\1"
        [ serializeFn $ payloadTuple payload
        , show (payloadPosition payload)
        , show (payloadLowWaterMarkMap payload) ]

deserializePayload :: String
                   -> (String -> PayloadTuple)
                   -> Payload
deserializePayload payloadStr deserializeFn =
    let [sTuple, sPos, sLWMMap] = splitOn "\1" payloadStr
        tuple = deserializeFn sTuple
        position = read sPos :: (Partition, Offset)
        lwmMap = read sLWMMap :: Map.Map ProcessorName Clock
    in Payload tuple position lwmMap
