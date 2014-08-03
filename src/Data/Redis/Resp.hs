-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module Data.Redis.Resp (RespData (..), decode, encode) where

import Control.Applicative
import Control.Monad (replicateM, void)
import Data.Attoparsec.ByteString.Char8 hiding (char8)
import Data.ByteString (ByteString)
import Data.ByteString.Builder
import Data.Foldable
import Data.Int
import Data.Monoid
import Data.Sequence (Seq)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Prelude hiding (foldr, take, concatMap)

import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Sequence        as Seq

data RespData
    = RespStr   !Text
    | RespErr   !Text
    | RespInt   !Int64
    | RespBulk  ByteString
    | RespArray (Seq RespData)
    | RespNull  Null
    deriving (Eq, Ord, Show)

data Null
    = A -- ^ null array
    | B -- ^ null bulk string
    deriving (Eq, Ord, Show)

decode :: ByteString -> Either String RespData
decode = parseOnly single

encode :: RespData -> Lazy.ByteString
encode d = toLazyByteString (go d)
  where
    go (RespStr   x) = char8 '+' <> byteString (encodeUtf8 x) <> crlf'
    go (RespErr   x) = char8 '-' <> byteString (encodeUtf8 x) <> crlf'
    go (RespInt   x) = char8 ':' <> int64Dec x <> crlf'
    go (RespNull  A) = nullArray
    go (RespNull  B) = nullBulk
    go (RespBulk  x) = char8 '$'
        <> intDec (B.length x)
        <> crlf'
        <> byteString x
        <> crlf'
    go (RespArray x) = char8 '*'
        <> intDec (Seq.length x)
        <> crlf'
        <> foldr (<>) mempty (fmap go x)

-----------------------------------------------------------------------------
-- Parsing

single :: Parser RespData
single = do
    t <- anyChar
    case t of
        '+' -> RespStr `fmap` text         <* crlf
        '-' -> RespErr `fmap` text         <* crlf
        ':' -> RespInt <$> signed decimal  <* crlf
        '$' -> bulk                        <* crlf
        '*' -> array
        _   -> fail $ "invalid type tag: " ++ show t

bulk :: Parser RespData
bulk = do
    n <- signed decimal <* crlf
    if | n >=  0   -> RespBulk <$> take n
       | n == -1   -> return (RespNull B)
       | otherwise -> fail "negative bulk length"

array :: Parser RespData
array = do
    n <- signed decimal <* crlf :: Parser Int
    if | n >=  0   -> RespArray . Seq.fromList <$> replicateM n single
       | n == -1   -> return (RespNull A)
       | otherwise -> fail "negative array length"

text :: Parser Text
text = decodeUtf8 <$> takeWhile1 (/= '\r')

crlf :: Parser ()
crlf = void $ string "\r\n"

-----------------------------------------------------------------------------
-- Serialising

nullArray, nullBulk, crlf' :: Builder
nullArray = byteString "*-1\r\n"
nullBulk  = byteString "$-1\r\n"
crlf'     = byteString "\r\n"
