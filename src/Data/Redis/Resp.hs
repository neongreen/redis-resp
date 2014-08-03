-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module Data.Redis.Resp
    ( Resp (..)
    , resp
    , decode
    , encode
    ) where

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
import Prelude hiding (foldr, take)

import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Sequence        as Seq

data Resp
    = Str   !Text
    | Err   !Text
    | Int   !Int64
    | Bulk  !ByteString
    | Array (Seq Resp)
    | NullArray
    | NullBulk
    deriving (Eq, Ord, Show)

decode :: ByteString -> Either String Resp
decode = parseOnly resp

encode :: Resp -> Lazy.ByteString
encode d = toLazyByteString (go d)
  where
    go (Str   x) = char8 '+' <> byteString (encodeUtf8 x) <> crlf'
    go (Err   x) = char8 '-' <> byteString (encodeUtf8 x) <> crlf'
    go (Int   x) = char8 ':' <> int64Dec x <> crlf'
    go (Bulk  x) = char8 '$'
        <> intDec (B.length x)
        <> crlf'
        <> byteString x
        <> crlf'
    go (Array x) = char8 '*'
        <> intDec (Seq.length x)
        <> crlf'
        <> foldr (<>) mempty (fmap go x)
    go NullArray = nullArray
    go NullBulk  = nullBulk

-----------------------------------------------------------------------------
-- Parsing

resp :: Parser Resp
resp = do
    t <- anyChar
    case t of
        '+' -> Str `fmap` text         <* crlf
        '-' -> Err `fmap` text         <* crlf
        ':' -> Int <$> signed decimal  <* crlf
        '$' -> bulk
        '*' -> array
        _   -> fail $ "invalid type tag: " ++ show t

bulk :: Parser Resp
bulk = do
    n <- signed decimal <* crlf
    if | n >=  0   -> Bulk <$> take n <* crlf
       | n == -1   -> return NullBulk
       | otherwise -> fail "negative bulk length"

array :: Parser Resp
array = do
    n <- signed decimal <* crlf :: Parser Int
    if | n >=  0   -> Array . Seq.fromList <$> replicateM n resp
       | n == -1   -> return NullArray
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
