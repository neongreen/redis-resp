-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}

module Data.Redis.Command where

import Control.Monad.Operational
import Data.ByteString (ByteString)
import Data.ByteString.From
import Data.DList hiding (singleton)
import Data.Int
import Data.Monoid
import Data.Redis.Resp
import Data.String
import GHC.TypeLits

import qualified Data.DList as DL

data Error
    = Error             !ByteString
    | WrongType
    | InvalidResponse   !String
    | InvalidConversion !String
    deriving (Eq, Ord, Show)

type Redis e = ProgramT (Command e)
type Result = Either Error

-- | Redis commands
data Command e r where
    Ping :: Resp -> Command e (e (Result Pong))
    Get  :: FromByteString a => Resp -> Command e (e (Result a))
    Set  :: Resp -> Command e (e (Result Bool))

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

ping :: Monad m => Redis e m (e (Result Pong))
ping = singleton $ Ping (Array 1 [Bulk "PING"])

fromPing :: Resp -> Result Pong
fromPing (Str "PONG") = Right Pong
fromPing _            = Left $ InvalidResponse "ping"

get :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result a))
get k = singleton $ Get (Array 2 [Bulk "GET", Bulk (key k)])

fromGet :: FromByteString a => Resp -> Result a
fromGet (Str  s) = either (Left . InvalidConversion) Right $ runParser parser s
fromGet (Bulk s) = either (Left . InvalidConversion) Right $ runParser parser s
fromGet _        = Left $ InvalidResponse "get"

set :: Monad m => Key -> ByteString -> Opts "SET" -> Redis e m (e (Result Bool))
set k v o = singleton $ Set $ Array (3 + len o)
    $ Bulk "SET"
    : Bulk (key k)
    : Bulk v
    : toList (opts o)

fromSet :: Resp -> Result Bool
fromSet (Str "OK") = Right True
fromSet NullBulk   = Right False
fromSet _          = Left $ InvalidResponse "set"

ex :: Int64 -> Opts "SET"
ex i = Opts 2 $ Bulk "EX" `cons` DL.singleton (Int i)

px :: Int64 -> Opts "SET"
px i = Opts 2 $ Bulk "PX" `cons` DL.singleton (Int i)

xx :: Opts "SET"
xx = Opts 1 $ DL.singleton (Bulk "XX")

nx :: Opts "SET"
nx = Opts 1 $ DL.singleton (Bulk "NX")
