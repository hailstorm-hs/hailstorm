module Hailstorm.Processors where

import Hailstorm.UserFormula
import qualified Data.ByteString as B

runSpout :: (Show k, Show v) => (UserFormula k v) -> IO ()
runSpout uf = do
    let (k,v) = convertFn uf $ B.empty
    print k
    print v
