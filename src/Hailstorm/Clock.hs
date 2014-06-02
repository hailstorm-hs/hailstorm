module Hailstorm.Clock
( Clock(..)
, Partition
, Offset
, clockGt
) where

import qualified Data.Map as Map

type Partition = String
type Offset = Integer

newtype Clock = Clock {extractClockMap :: Map.Map Partition Offset}
                deriving (Eq, Show, Read, Ord)

-- | @c1 `clockGt` @c2@ returns true if @c1@ has the same elements
-- as @c2@ and is element-wise greater than @c2@.
clockGt :: Clock -> Clock -> Bool
clockGt (Clock clk1) (Clock clk2) =
    let (x:xs) `gtElems` (y:ys) = x > y && xs `gtElems` ys
        [] `gtElems` [] = True
        _ `gtElems` _ = False
    in (Map.keys clk1 == Map.keys clk2) &&
        (Map.elems clk1 `gtElems` Map.elems clk2)
