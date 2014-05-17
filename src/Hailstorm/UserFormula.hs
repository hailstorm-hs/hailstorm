{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.UserFormula where

import Data.Monoid
import qualified Data.ByteString as B

-- | A formula for stream computation over key-value tuples. Keys @k@ must
-- be of instances of Ord and values @v@ must be instances of Monoid.
data UserFormula k v = (Ord k, Monoid v) => UserFormula
  { convertFn :: B.ByteString -> (k,v)
  , outputFn  :: (k,v) -> IO () 
  , serialize :: (k,v) -> String
  , deserialize :: String -> (k, v)
  }

-- | Formula that defaults serialize / deserialize to read and show.
-- This these fields are required because of existential types
newUserFormula :: (Show k, Show v, Read k, Read v, Ord k, Monoid v) => (B.ByteString -> (k,v)) -> ((k,v) -> IO ()) -> UserFormula k v
newUserFormula convert output = UserFormula {
          convertFn = convert
        , outputFn = output
        , serialize = show
        , deserialize = read
    }
