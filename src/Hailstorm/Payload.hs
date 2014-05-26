module Hailstorm.Payload
( Payload(..)
) where

import Hailstorm.Clock

data Payload k v = Payload
    { payloadTuple :: (k,v)
    , payloadClock :: Clock
    } deriving (Eq, Show, Read)
