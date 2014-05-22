{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Error 
( HSError (..)
, forceEitherIO
) where

import Control.Exception
import Data.Typeable

data HSError = UnknownWorkerException
             | DuplicateNegotiatorError String
             | ZookeeperConnectionError String
             | HSErrorWrap HSError String
    deriving (Show, Typeable)

instance Exception HSError 
    
forceEitherIO:: (Show e) => HSError -> IO (Either e a) -> IO (a)
forceEitherIO mye action = do
    me <- action
    case me of 
        (Left e) -> throw $ HSErrorWrap mye (show e) 
        (Right x) -> return x
