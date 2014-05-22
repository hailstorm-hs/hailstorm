{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Error 
( HSError (..)
) where

import Control.Exception
import Data.Typeable

data HSError = UnknownWorkerException
             | DuplicateNegotiatorError String
             | ZookeeperConnectionError String
    deriving (Show, Typeable)

instance Exception HSError 
