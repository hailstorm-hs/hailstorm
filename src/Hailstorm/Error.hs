{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Error
( HSError (..)
, doubleThrow
, forceEitherIO
, warnOnLeftIO
, wrapInHSError
) where

import Control.Concurrent
import Control.Exception
import Data.Typeable
import System.Log.Logger

data HSError = UnknownWorkerException
             | BadStateError String
             | UnexpectedZookeeperError
             | BadStartupError String
             | BadClusterStateError String
             | InvalidTopologyError String
             | DuplicateNegotiatorError String
             | ZookeeperConnectionError String
             | RegistrationDeleted
             | HSErrorWrap HSError String
             | UnexpectedKafkaError
             | DisconnectionError String
               deriving (Show, Typeable)
instance Exception HSError

wrapInHSError :: (Show e) => e -> HSError -> HSError
wrapInHSError e hs = HSErrorWrap hs (show e)

forceEitherIO :: (Show e) => HSError -> IO (Either e a) -> IO a
forceEitherIO mye action = do
    me <- action
    case me of
        Left e -> throw $ HSErrorWrap mye (show e)
        Right x -> return x

warnOnLeftIO :: (Show e) => IO (Either e a) -> IO ()
warnOnLeftIO etAction = do
    et <- etAction
    case et of
        Left e -> warningM "Hailstorm.Error" $ "Warning: " ++ show e
        _ -> return ()

doubleThrow :: (Exception e) => ThreadId -> e -> IO ()
doubleThrow tid e = throwTo tid e >> throw e
