{-# LANGUAGE ExistentialQuantification #-}
module UserFormula where

import Data.Monoid
import qualified Data.ByteString as B

data UserFormula k v = (Ord k, Monoid v) => UserFormula
  { convert :: B.ByteString -> (k,v)
  , output :: (k,v) -> IO () }
