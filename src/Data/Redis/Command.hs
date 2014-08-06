-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE StandaloneDeriving         #-}

module Data.Redis.Command
    ( RunCommand (..)
    , Command    (..)
    , Args
    , Key
    , ping
    , get
    , set, ex, px, nx, xx
    ) where

import Data.ByteString (ByteString)
import Data.Int
import Data.Monoid
import Data.Redis.Resp
import Data.String
import Data.Sequence (Seq, singleton, fromList, (|>))
import GHC.TypeLits

data Command a where
    Ping :: Resp -> Command ByteString
    Get  :: Resp -> Command (Maybe ByteString)
    Set  :: Resp -> Command Bool

deriving instance Eq   (Command a)
deriving instance Show (Command a)

newtype Args (a :: Symbol) = Args { args :: Seq Resp }
    deriving Monoid

newtype Key = Key ByteString
    deriving (Eq, Ord, Show)

instance IsString Key where
    fromString = Key . fromString

class RunCommand m where
    runCommand :: Command a -> m a

ping :: RunCommand m => m ByteString
ping = runCommand . Ping . Array $ fromList [ Bulk "PING" ]

get :: RunCommand m => Key -> m (Maybe ByteString)
get (Key k) = runCommand . Get . Array $ fromList [ Bulk "GET", Bulk k ]

set :: RunCommand m => Key -> ByteString -> Args "SET" -> m Bool
set (Key k) v a = runCommand . Set . Array $ fromList
    [ Bulk "SET", Bulk k, Bulk v ] <> args a

ex :: Int64 -> Args "SET"
ex i = Args $ singleton (Bulk "EX") |> Int i

px :: Int64 -> Args "SET"
px i = Args $ singleton (Bulk "PX") |> Int i

xx :: Args "SET"
xx = Args $ singleton (Bulk "XX")

nx :: Args "SET"
nx = Args $ singleton (Bulk "NX")
