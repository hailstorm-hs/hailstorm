{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.TransactionTypes
( PayloadTuple(..)
, BoltState(..)
) where

import Data.Dynamic

data PayloadTuple = MkPayloadTuple Dynamic
  deriving (Show)

data BoltState = MkBoltState Dynamic
  deriving (Show)
