module Hailstorm.Payload
( Payload(..)
, serializePayload
, deserializePayload
) where

import Data.List
import Data.List.Split
import Hailstorm.Clock
import Hailstorm.InputSource
import Hailstorm.UserFormula

data Payload k v = Payload
    { payloadTuple :: (k, v)
    , payloadPosition :: (Partition, Offset)
    , payloadLowWaterMark :: Clock
    } deriving (Eq, Show, Read)

serializePayload :: Payload k v
                 -> UserFormula k v
                 -> String
serializePayload payload uFormula =
    intercalate "\1"
        [ serialize uFormula (payloadTuple payload)
        , show (payloadPosition payload)
        , show (payloadLowWaterMark payload) ]

deserializePayload :: String
                   -> UserFormula k v
                   -> Payload k v
deserializePayload payloadStr uFormula =
    let [sTuple, sPos, sLWM] = splitOn "\1" payloadStr
        tuple = deserialize uFormula sTuple
        position = read sPos :: (Partition, Offset)
        lwm = read sLWM :: Clock
    in Payload tuple position lwm
