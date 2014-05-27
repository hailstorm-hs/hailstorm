module Hailstorm.Logging
( initializeLogging
) where

import System.IO
import System.Log.Formatter
import System.Log.Logger
import System.Log.Handler (setFormatter)
import System.Log.Handler.Simple

initializeLogging :: IO ()
initializeLogging = do
    hdlr <- streamHandler stderr INFO
    let fmtr = simpleLogFormatter "[$loggername $prio] $msg"
    updateGlobalLogger rootLoggerName $
        setLevel INFO . setHandlers [setFormatter hdlr fmtr]
