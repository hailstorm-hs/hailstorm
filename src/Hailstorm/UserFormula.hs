{-# LANGUAGE ExistentialQuantification #-}
module Hailstorm.UserFormula where

import Data.Monoid
import qualified Data.ByteString as B
import qualified Data.Map as Map

-- | A formula for stream computation over key-value tuples. Keys @k@ must
-- be of instances of Ord and values @v@ must be instances of Monoid.
-- TODO: once serialize and deserialize no longer use Show/Read, remove
-- Show/Read restriction on k and v.
data UserFormula k v = (Show k, Show v, Read k, Read v, Ord k, Monoid v) =>
  UserFormula
  { convertFn :: B.ByteString -> (k,v)
  , outputFn  :: (k,v) -> IO ()
  , serialize :: (k,v) -> String
  , deserialize :: String -> (k, v)
  , deserializeState :: String -> Map.Map k v
  }

-- | Formula that defaults serialize / deserialize to read and show.
-- TODO: use a better way to serialize data instead of read and show.
newUserFormula :: (Show k, Show v, Read k, Read v, Ord k, Monoid v)
               => (B.ByteString -> (k,v)) -> ((k,v) -> IO ())
               -> UserFormula k v
newUserFormula convert output = UserFormula { convertFn = convert
                                            , outputFn = output
                                            , serialize = show
                                            , deserialize = read
                                            , deserializeState = read
                                            }
