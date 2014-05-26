{-# LANGUAGE DeriveDataTypeable #-}
module Hailstorm.Error
( HSError (..)
, forceEitherIO
, warnOnLeftIO
, wrapInHSError
) where

import Control.Exception
import Data.Typeable
import System.IO

data HSError = UnknownWorkerException
             | UnexpectedZookeeperError
             | UnexpectedLeakError
             | InvalidTopologyError String
             | DuplicateNegotiatorError String
             | ZookeeperConnectionError String
             | HSErrorWrap HSError String
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
        Left e -> hPutStrLn stderr $ "Error: " ++ show e
        _ -> return ()
