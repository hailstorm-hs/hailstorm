module Hailstorm.Payload
( Payload(..)
, serializePayload
, deserializePayload
) where

import Data.List
import Data.List.Split
import Hailstorm.Clock
import Hailstorm.UserFormula
import Hailstorm.Processor
import qualified Data.Map as Map

data Payload k v = Payload
    { payloadTuple :: (k, v)
    , payloadPosition :: (Partition, Offset)
    , payloadLowWaterMarkMap :: Map.Map ProcessorName Clock
    } deriving (Eq, Show, Read)

serializePayload :: Payload k v
                 -> UserFormula k v
                 -> String
serializePayload payload uFormula =
    intercalate "\1"
        [ serialize uFormula (payloadTuple payload)
        , show (payloadPosition payload)
        , show (payloadLowWaterMarkMap payload) ]

deserializePayload :: String
                   -> UserFormula k v
                   -> Payload k v
deserializePayload payloadStr uFormula =
    let [sTuple, sPos, sLWMMap] = splitOn "\1" payloadStr
        tuple = deserialize uFormula sTuple
        position = read sPos :: (Partition, Offset)
        lwmMap = read sLWMMap :: Map.Map ProcessorName Clock
    in Payload tuple position lwmMap
