module Hailstorm.Concurrency 
( threadDead
, waitForThreadDead
) where

import GHC.Conc
import Control.Monad.Loops

threadDead :: ThreadId -> IO Bool
threadDead = fmap (\x -> x == ThreadDied || x == ThreadFinished) . threadStatus

waitForThreadDead :: ThreadId -> IO ()
waitForThreadDead tid = untilM_ (threadDelay $ 100 * 1000) (threadDead tid) 
