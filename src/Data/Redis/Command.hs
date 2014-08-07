-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}

module Data.Redis.Command
    ( Command (..)
    , Opts
    , Key
    , ping
    , get
    , set, ex, px, nx, xx
    ) where

import Control.Monad.Free
import Data.ByteString (ByteString)
import Data.DList
import Data.Int
import Data.IORef
import Data.Monoid
import Data.Redis.Resp
import Data.String
import GHC.TypeLits

-- | Redis commands
data Command k
    = Ping !Resp (IORef Resp -> k)
    | Get  !Resp (IORef Resp -> k)
    | Set  !Resp (IORef Resp -> k)
    deriving Functor

-- | Redis key type
newtype Key = Key { key :: ByteString } deriving (Eq, Ord, Show)

instance IsString Key where
    fromString = Key . fromString

-- | Command options
data Opts (a :: Symbol) = Opts { len :: !Int, opts :: DList Resp }

instance Monoid (Opts a) where
    mempty = Opts 0 empty
    Opts x a `mappend` Opts y b = Opts (x + y) (a `append` b)

ping :: Free Command (IORef Resp)
ping = liftF $ Ping (Array 1 [Bulk "PING"]) id

get :: Key -> Free Command (IORef Resp)
get k = liftF $ Get (Array 2 [Bulk "GET", Bulk (key k)]) id

set :: Key -> ByteString -> Opts "SET" -> Free Command (IORef Resp)
set k v o = liftF $ Set arr id
  where
    arr = Array (3 + len o)
        $ Bulk "SET"
        : Bulk (key k)
        : Bulk v
        : toList (opts o)

ex :: Int64 -> Opts "SET"
ex i = Opts 2 $ Bulk "EX" `cons` singleton (Int i)

px :: Int64 -> Opts "SET"
px i = Opts 2 $ Bulk "PX" `cons` singleton (Int i)

xx :: Opts "SET"
xx = Opts 1 $ singleton (Bulk "XX")

nx :: Opts "SET"
nx = Opts 1 $ singleton (Bulk "NX")
