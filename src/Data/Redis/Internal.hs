-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Data.Redis.Internal where

import Control.Applicative
import Data.IORef

newtype Lazy a = Lazy { thunk :: IORef (Thunk a) }

data Thunk a
    = Thunk (IO a)
    | Value !a

lazy :: IO a -> IO (Lazy a)
lazy a = Lazy <$> newIORef (Thunk a)

force :: Lazy a -> IO a
force (Lazy f) = do
    s <- readIORef f
    case s of
        Value a -> return a
        Thunk a -> do
            x <- a
            writeIORef f (Value x)
            return x
