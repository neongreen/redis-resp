-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Data.Redis.Command where

import Control.Monad.IO.Class
import Control.Monad.Trans.Free
import Data.ByteString (ByteString)
import Data.DList
import Data.Int
import Data.Monoid
import Data.Redis.Resp
import Data.Redis.Internal
import Data.String
import GHC.TypeLits

data Error
    = Error           !ByteString
    | WrongType
    | InvalidResponse !ByteString
    deriving (Eq, Ord, Show)

type Result = Either Error

-- | Redis commands
data Command k where
    Ping :: !Resp -> (Lazy (Result Pong) -> k) -> Command k
    Get  :: !Resp -> (Lazy (Result Resp) -> k) -> Command k
    Set  :: !Resp -> (Lazy (Result Bool) -> k) -> Command k

deriving instance Functor Command

data Pong = Pong deriving (Eq, Show)

-- | Redis key type
newtype Key = Key { key :: ByteString } deriving (Eq, Ord, Show)

instance IsString Key where
    fromString = Key . fromString

-- | Command options
data Opts (a :: Symbol) = Opts { len :: !Int, opts :: DList Resp }

instance Monoid (Opts a) where
    mempty = Opts 0 empty
    Opts x a `mappend` Opts y b = Opts (x + y) (a `append` b)

await :: MonadIO m => Lazy (Result a) -> FreeT Command m (Result a)
await r = liftIO $ force r

ping :: Monad m => FreeT Command m (Lazy (Result Pong))
ping = liftF $ Ping (Array 1 [Bulk "PING"]) id

fromPing :: Resp -> Result Pong
fromPing (Str "PONG") = Right Pong
fromPing _            = Left $ InvalidResponse "ping"

get :: Monad m => Key -> FreeT Command m (Lazy (Result Resp))
get k = liftF $ Get (Array 2 [Bulk "GET", Bulk (key k)]) id

fromGet :: Resp -> Result Resp
fromGet = Right

set :: Monad m => Key -> ByteString -> Opts "SET" -> FreeT Command m (Lazy (Result Bool))
set k v o = liftF $ Set arr id
  where
    arr = Array (3 + len o)
        $ Bulk "SET"
        : Bulk (key k)
        : Bulk v
        : toList (opts o)

fromSet :: Resp -> Result Bool
fromSet (Str "OK") = Right True
fromSet NullBulk   = Right False
fromSet _          = Left $ InvalidResponse "set"

ex :: Int64 -> Opts "SET"
ex i = Opts 2 $ Bulk "EX" `cons` singleton (Int i)

px :: Int64 -> Opts "SET"
px i = Opts 2 $ Bulk "PX" `cons` singleton (Int i)

xx :: Opts "SET"
xx = Opts 1 $ singleton (Bulk "XX")

nx :: Opts "SET"
nx = Opts 1 $ singleton (Bulk "NX")
