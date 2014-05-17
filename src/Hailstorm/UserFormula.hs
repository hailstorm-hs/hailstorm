{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.UserFormula where

import Data.Monoid
import qualified Data.ByteString as B

-- | A formula for stream computation over key-value tuples. Keys @k@ must
-- be of instances of Ord and values @v@ must be instances of Monoid.
data UserFormula k v = (Ord k, Monoid v) => UserFormula
  { convertFn :: B.ByteString -> (k,v)
  , serialize :: (k,v) -> String
  , deserialize :: String -> (k, v)
  , outputFn  :: (k,v) -> IO () }
