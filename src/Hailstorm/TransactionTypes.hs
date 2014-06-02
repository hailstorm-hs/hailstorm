{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.TransactionTypes
( PayloadTuple(..)
, BoltState(..)
) where

import Data.Dynamic

data PayloadTuple = MkPayloadTuple Dynamic

data BoltState = MkBoltState Dynamic
