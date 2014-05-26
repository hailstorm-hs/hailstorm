module Hailstorm.Payload
( Payload(..)
, payloadProducer
) where

import Pipes
import Hailstorm.Clock
import Hailstorm.UserFormula
import Hailstorm.InputSource
import qualified Data.Map as Map

data Payload k v = Payload
    { payloadTuple :: (k,v)
    , payloadClock :: Clock
    } deriving (Eq, Show, Read)

payloadProducer :: Monad m
                => UserFormula k v
                -> Producer InputTuple m ()
                -> Producer (Payload k v) m ()
payloadProducer uformula tupleProducer =
    let processProducer (InputTuple bs p o) = yield $
            Payload (convertFn uformula bs) (Clock $ Map.singleton p o)
    in for tupleProducer processProducer

