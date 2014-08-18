-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TupleSections              #-}

module Data.Redis.Command
    ( -- * Types
      Redis
    , PubSub
    , Command       (..)
    , PubSubCommand (..)
    , PushMessage   (..)
    , Result
    , RedisError    (..)
    , RedisType     (..)
    , TTL           (..)
    , Side          (..)
    , Choose        (..)
    , Aggregate     (..)
    , Min           (..)
    , Max           (..)
    , ScoreList     (..)
    , Seconds       (..)
    , Timestamp     (..)
    , Field
    , Index
    , Key           (..)

    -- ** Cursor
    , Cursor
    , zero

    -- ** Non-empty lists
    , NonEmpty (..)
    , one

    -- ** Options
    , Opts
    , none

    -- ** Bit
    , BitStart
    , BitEnd
    , start
    , end

    -- * Commands
    -- ** Connection
    , auth
    , echo
    , ping
    , quit
    , select

    -- ** Server
    , bgrewriteaof
    , bgsave
    , dbsize
    , flushall
    , flushdb
    , lastsave
    , save

    -- ** Transactions
    , discard
    , exec
    , execRaw
    , multi
    , unwatch
    , watch

    -- ** Keys
    , del
    , dump
    , exists
    , expire
    , expireat
    , keys
    , persist
    , randomkey
    , rename
    , renamenx
    , ttl
    , typeof

    -- ** Strings
    , append
    , decr
    , decrby
    , get
    , getrange
    , getset
    , incr
    , incrby
    , incrbyfloat
    , mget
    , mset
    , msetnx
    , set, ex, px, xx, nx
    , setrange
    , strlen

    -- ** Bits
    , bitand
    , bitcount, range
    , bitnot
    , bitor
    , bitpos
    , bitxor
    , getbit
    , setbit

    -- ** Hashes
    , hdel
    , hexists
    , hget
    , hgetall
    , hincrby
    , hincrbyfloat
    , hkeys
    , hlen
    , hmget
    , hmset
    , hset
    , hsetnx
    , hvals

    -- ** Lists
    , blpop
    , brpop
    , brpoplpush
    , lindex
    , linsert
    , llen
    , lpop
    , lpush
    , lpushx
    , lrange
    , lrem
    , lset
    , ltrim
    , rpop
    , rpoplpush
    , rpush
    , rpushx

    -- ** Sets
    , sadd
    , scard
    , sdiff
    , sdiffstore
    , sinter
    , sinterstore
    , sismember
    , smembers
    , smove
    , spop
    , srandmember
    , srem
    , sunion
    , sunionstore

    -- ** Sorted Sets
    , zadd
    , zcard
    , zcount
    , zincrby
    , zinterstore
    , zlexcount
    , zrange
    , zrangebylex
    , zrangebyscore
    , zrank
    , zrem
    , zremrangebylex
    , zremrangebyrank
    , zremrangebyscore
    , zrevrangebyscore
    , zrevrange
    , zrevrank
    , zscore
    , zunionstore

    -- ** HyperLogLog
    , pfadd
    , pfcount
    , pfmerge

    -- ** Scan
    , scan, match, count
    , hscan
    , sscan
    , zscan

    -- ** Sort
    , sort, by, limit, getkey, asc, desc, alpha, store

    -- ** Pub/Sub
    , publish
    , subscribe
    , psubscribe
    , unsubscribe
    , punsubscribe

    -- * Response Reading
    , readInt
    , readInt'Null
    , readBool
    , readTTL
    , readBulk'Null
    , readBulk
    , readListOfMaybes
    , readList
    , readScoreList
    , readFields
    , readKeyValue
    , readBulk'Array
    , readScan
    , matchStr
    , readType
    , fromSet
    , anyStr
    , readPushMessage
    ) where

import Control.Applicative
import Control.Exception (Exception)
import Control.Monad.Operational
import Data.ByteString (ByteString)
import Data.ByteString.Builder (int64Dec)
import Data.ByteString.Builder.Extra
import Data.ByteString.From
import Data.ByteString.Lazy (toStrict)
import Data.DList (DList, cons)
import Data.Double.Conversion.ByteString (toShortest)
import Data.Foldable (toList, foldr)
import Data.Int
import Data.List.NonEmpty (NonEmpty (..), (<|))
import Data.List.Split (chunksOf)
import Data.Maybe (maybeToList)
import Data.Monoid hiding (Sum)
import Data.Redis.Resp
import Data.String
import Data.Typeable
import GHC.TypeLits
import Prelude hiding (foldr, readList)

import qualified Data.ByteString    as B
import qualified Data.DList         as DL
import qualified Data.List.NonEmpty as NE

-- | Redis error type.
data RedisError
    = RedisError !ByteString
        -- ^ General error case.
    | InvalidResponse !String
        -- ^ The received response is invalid or unexpected (e.g. a bulk
        -- string instead of an integer).
    | InvalidConversion !String
        -- ^ ByteString conversion using 'FromByteString' failed.
    deriving (Eq, Ord, Show, Typeable)

instance Exception RedisError

type Redis e = ProgramT (Command e)
type PubSub  = ProgramT PubSubCommand
type Result  = Either RedisError

-- | Redis commands.
data Command e r where
    -- Connection
    Ping   :: Resp -> Command e (e (Result ()))
    Echo   :: FromByteString a => Resp -> Command e (e (Result a))
    Auth   :: Resp -> Command e (e (Result ()))
    Quit   :: Resp -> Command e (e (Result ()))
    Select :: Resp -> Command e (e (Result ()))

    -- Server
    BgRewriteAOF :: Resp -> Command e (e (Result ()))
    BgSave       :: Resp -> Command e (e (Result ()))
    Save         :: Resp -> Command e (e (Result ()))
    DbSize       :: Resp -> Command e (e (Result Int64))
    FlushAll     :: Resp -> Command e (e (Result ()))
    FlushDb      :: Resp -> Command e (e (Result ()))
    LastSave     :: Resp -> Command e (e (Result Int64))

    -- Transactions
    Multi   :: Resp -> Command e (e (Result ()))
    Watch   :: Resp -> Command e (e (Result ()))
    Unwatch :: Resp -> Command e (e (Result ()))
    Discard :: Resp -> Command e (e (Result ()))
    Exec    :: FromByteString a => Resp -> Command e (e (Result [a]))
    ExecRaw :: Resp -> Command e (e (Result Resp))

    -- Keys
    Del       :: Resp -> Command e (e (Result Int64))
    Dump      :: Resp -> Command e (e (Result (Maybe ByteString)))
    Exists    :: Resp -> Command e (e (Result Bool))
    Expire    :: Resp -> Command e (e (Result Bool))
    ExpireAt  :: Resp -> Command e (e (Result Bool))
    Persist   :: Resp -> Command e (e (Result Bool))
    Keys      :: Resp -> Command e (e (Result [Key]))
    RandomKey :: Resp -> Command e (e (Result (Maybe Key)))
    Rename    :: Resp -> Command e (e (Result ()))
    RenameNx  :: Resp -> Command e (e (Result Bool))
    Sort      :: FromByteString a => Resp -> Command e (e (Result [a]))
    Ttl       :: Resp -> Command e (e (Result (Maybe TTL)))
    Type      :: Resp -> Command e (e (Result (Maybe RedisType)))
    Scan      :: FromByteString a => Resp -> Command e (e (Result (Cursor, [a])))

    -- Strings
    Append   :: Resp -> Command e (e (Result Int64))
    Get      :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    GetRange :: FromByteString a => Resp -> Command e (e (Result a))
    GetSet   :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    MGet     :: FromByteString a => Resp -> Command e (e (Result [Maybe a]))
    MSet     :: Resp -> Command e (e (Result ()))
    MSetNx   :: Resp -> Command e (e (Result Bool))
    Set      :: Resp -> Command e (e (Result Bool))
    SetRange :: Resp -> Command e (e (Result Int64))
    StrLen   :: Resp -> Command e (e (Result Int64))

    -- Bits
    BitAnd   :: Resp -> Command e (e (Result Int64))
    BitCount :: Resp -> Command e (e (Result Int64))
    BitNot   :: Resp -> Command e (e (Result Int64))
    BitOr    :: Resp -> Command e (e (Result Int64))
    BitPos   :: Resp -> Command e (e (Result Int64))
    BitXOr   :: Resp -> Command e (e (Result Int64))
    GetBit   :: Resp -> Command e (e (Result Int64))
    SetBit   :: Resp -> Command e (e (Result Int64))

    -- Numeric
    Decr        :: Resp -> Command e (e (Result Int64))
    DecrBy      :: Resp -> Command e (e (Result Int64))
    Incr        :: Resp -> Command e (e (Result Int64))
    IncrBy      :: Resp -> Command e (e (Result Int64))
    IncrByFloat :: Resp -> Command e (e (Result Double))

    -- Hashes
    HDel         :: Resp -> Command e (e (Result Int64))
    HExists      :: Resp -> Command e (e (Result Bool))
    HGet         :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    HGetAll      :: FromByteString a => Resp -> Command e (e (Result [(Field, a)]))
    HIncrBy      :: Resp -> Command e (e (Result Int64))
    HIncrByFloat :: Resp -> Command e (e (Result Double))
    HKeys        :: Resp -> Command e (e (Result [Field]))
    HLen         :: Resp -> Command e (e (Result Int64))
    HMGet        :: FromByteString a => Resp -> Command e (e (Result [Maybe a]))
    HMSet        :: Resp -> Command e (e (Result ()))
    HSet         :: Resp -> Command e (e (Result Bool))
    HSetNx       :: Resp -> Command e (e (Result Bool))
    HVals        :: FromByteString a => Resp -> Command e (e (Result [a]))
    HScan        :: FromByteString a => Resp -> Command e (e (Result (Cursor, [a])))

    -- Lists
    BLPop      :: FromByteString a => Int64 -> Resp -> Command e (e (Result (Maybe (Key, a))))
    BRPop      :: FromByteString a => Int64 -> Resp -> Command e (e (Result (Maybe (Key, a))))
    BRPopLPush :: FromByteString a => Int64 -> Resp -> Command e (e (Result (Maybe a)))
    LIndex     :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    LInsert    :: Resp -> Command e (e (Result Int64))
    LLen       :: Resp -> Command e (e (Result Int64))
    LPop       :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    LPush      :: Resp -> Command e (e (Result Int64))
    LPushX     :: Resp -> Command e (e (Result Int64))
    LRange     :: FromByteString a => Resp -> Command e (e (Result [a]))
    LRem       :: Resp -> Command e (e (Result Int64))
    LSet       :: Resp -> Command e (e (Result ()))
    LTrim      :: Resp -> Command e (e (Result ()))
    RPop       :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    RPopLPush  :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    RPush      :: Resp -> Command e (e (Result Int64))
    RPushX     :: Resp -> Command e (e (Result Int64))

    -- Sets
    SAdd        :: Resp -> Command e (e (Result Int64))
    SCard       :: Resp -> Command e (e (Result Int64))
    SDiff       :: FromByteString a => Resp -> Command e (e (Result [a]))
    SDiffStore  :: Resp -> Command e (e (Result Int64))
    SInter      :: FromByteString a => Resp -> Command e (e (Result [a]))
    SInterStore :: Resp -> Command e (e (Result Int64))
    SIsMember   :: Resp -> Command e (e (Result Bool))
    SMembers    :: FromByteString a => Resp -> Command e (e (Result [a]))
    SMove       :: Resp -> Command e (e (Result Bool))
    SPop        :: FromByteString a => Resp -> Command e (e (Result (Maybe a)))
    SRandMember :: FromByteString a => Choose -> Resp -> Command e (e (Result [a]))
    SRem        :: Resp -> Command e (e (Result Int64))
    SScan       :: FromByteString a => Resp -> Command e (e (Result (Cursor, [a])))
    SUnion      :: FromByteString a => Resp -> Command e (e (Result [a]))
    SUnionStore :: Resp -> Command e (e (Result Int64))

    -- Sorted Sets
    ZAdd             :: Resp -> Command e (e (Result Int64))
    ZCard            :: Resp -> Command e (e (Result Int64))
    ZCount           :: Resp -> Command e (e (Result Int64))
    ZIncrBy          :: Resp -> Command e (e (Result Double))
    ZInterStore      :: Resp -> Command e (e (Result Int64))
    ZLexCount        :: Resp -> Command e (e (Result Int64))
    ZRange           :: FromByteString a => Bool -> Resp -> Command e (e (Result (ScoreList a)))
    ZRangeByLex      :: FromByteString a => Resp -> Command e (e (Result [a]))
    ZRangeByScore    :: FromByteString a => Bool -> Resp -> Command e (e (Result (ScoreList a)))
    ZRank            :: Resp -> Command e (e (Result (Maybe Int64)))
    ZRem             :: Resp -> Command e (e (Result Int64))
    ZRemRangeByLex   :: Resp -> Command e (e (Result Int64))
    ZRemRangeByRank  :: Resp -> Command e (e (Result Int64))
    ZRemRangeByScore :: Resp -> Command e (e (Result Int64))
    ZRevRange        :: FromByteString a => Bool -> Resp -> Command e (e (Result (ScoreList a)))
    ZRevRangeByScore :: FromByteString a => Bool -> Resp -> Command e (e (Result (ScoreList a)))
    ZRevRank         :: Resp -> Command e (e (Result (Maybe Int64)))
    ZScan            :: FromByteString a => Resp -> Command e (e (Result (Cursor, [a])))
    ZScore           :: Resp -> Command e (e (Result (Maybe Double)))
    ZUnionStore      :: Resp -> Command e (e (Result Int64))

    -- HyperLogLog
    PfAdd   :: Resp -> Command e (e (Result Bool))
    PfCount :: Resp -> Command e (e (Result Int64))
    PfMerge :: Resp -> Command e (e (Result ()))

    -- Pub/Sub
    Publish :: Resp -> Command e (e (Result Int64))

-- | Pub/Sub commands.
data PubSubCommand r where
    Subscribe    :: Resp -> PubSubCommand ()
    Unsubscribe  :: Resp -> PubSubCommand ()
    PSubscribe   :: Resp -> PubSubCommand ()
    PUnsubscribe :: Resp -> PubSubCommand ()

data PushMessage
    = SubscribeMessage
        { channel       :: !ByteString
        , subscriptions :: !Int64
        }
    | UnsubscribeMessage
        { channel       :: !ByteString
        , subscriptions :: !Int64
        }
    | Message
        { channel :: !ByteString
        , message :: !ByteString
        }
    | PMessage
        { pattern :: !ByteString
        , channel :: !ByteString
        , message :: !ByteString
        }
    deriving (Eq, Ord, Show)

-- | The types redis reports via <http://redis.io/commands/type type>.
data RedisType
    = RedisString
    | RedisList
    | RedisSet
    | RedisZSet
    | RedisHash
    deriving (Eq, Ord, Show)

data TTL = NoTTL | TTL !Int64
    deriving (Eq, Ord, Show)

data Side = Before | After
    deriving (Eq, Ord, Show)

data Choose = One | Dist !Int64 | Arb !Int64
    deriving (Eq, Ord, Show)

data Aggregate = None | Min | Max | Sum
    deriving (Eq, Ord, Show)

data Min = MinIncl !ByteString | MinExcl !ByteString | MinInf
    deriving (Eq, Ord, Show)

data Max = MaxIncl !ByteString | MaxExcl !ByteString | MaxInf
    deriving (Eq, Ord, Show)

data ScoreList a = ScoreList
    { scores   :: [Double]
    , elements :: [a]
    } deriving (Eq, Ord, Show)

newtype Cursor = Cursor
    { cursor :: ByteString
    } deriving (Eq, Ord, Show, FromByteString)

zero :: Cursor
zero = Cursor "0"

type Field = ByteString
type Index = Int64

-- | Redis key type
newtype Key = Key
    { key :: ByteString
    } deriving (Eq, Ord, Show, FromByteString)

instance IsString Key where
    fromString = Key . fromString

-- | Command options
data Opts (a :: Symbol) = Opts { len :: !Int, opts :: DList ByteString }

instance Monoid (Opts a) where
    mempty = Opts 0 DL.empty
    Opts x a `mappend` Opts y b = Opts (x + y) (a `DL.append` b)

none :: Monoid m => m
none = mempty

newtype Seconds   = Seconds Int64
newtype Timestamp = Timestamp Int64
newtype BitStart  = BitStart ByteString
newtype BitEnd    = BitEnd   ByteString

instance Monoid BitStart where
    mempty        = BitStart ""
    _ `mappend` b = b

instance Monoid BitEnd where
    mempty        = BitEnd ""
    _ `mappend` b = b

start :: Int64 -> BitStart
start = BitStart . int2bytes

end :: Int64 -> BitEnd
end = BitEnd . int2bytes

one :: a -> NonEmpty a
one a = a :| []

-----------------------------------------------------------------------------
-- Connection

ping :: Monad m => Redis e m (e (Result ()))
ping = singleton $ Ping $ cmd 1 ["PING"]

echo :: (Monad m, FromByteString a) => ByteString -> Redis e m (e (Result a))
echo x = singleton $ Echo $ cmd 2 ["ECHO", x]

auth :: Monad m => ByteString -> Redis e m (e (Result ()))
auth x = singleton $ Auth $ cmd 2 ["AUTH", x]

quit :: Monad m => Redis e m (e (Result ()))
quit = singleton $ Quit $ cmd 1 ["QUIT"]

select :: Monad m => Int64 -> Redis e m (e (Result ()))
select x = singleton $ Select $ cmd 2 ["SELECT", int2bytes x]

-----------------------------------------------------------------------------
-- Server

bgrewriteaof :: Monad m => Redis e m (e (Result ()))
bgrewriteaof = singleton $ BgRewriteAOF $ cmd 1 ["BGREWRITEAOF"]

bgsave :: Monad m => Redis e m (e (Result ()))
bgsave = singleton $ BgSave $ cmd 1 ["BGSAVE"]

save :: Monad m => Redis e m (e (Result ()))
save = singleton $ Save $ cmd 1 ["SAVE"]

flushall :: Monad m => Redis e m (e (Result ()))
flushall = singleton $ FlushAll $ cmd 1 ["FLUSHALL"]

flushdb :: Monad m => Redis e m (e (Result ()))
flushdb = singleton $ FlushDb $ cmd 1 ["FLUSHDB"]

lastsave :: Monad m => Redis e m (e (Result Int64))
lastsave = singleton $ LastSave $ cmd 1 ["LASTSAVE"]

dbsize :: Monad m => Redis e m (e (Result Int64))
dbsize = singleton $ DbSize $ cmd 1 ["DBSIZE"]

-----------------------------------------------------------------------------
-- Transactions

multi :: Monad m => Redis e m (e (Result ()))
multi = singleton $ Multi $ cmd 1 ["MULTI"]

discard :: Monad m => Redis e m (e (Result ()))
discard = singleton $ Discard $ cmd 1 ["DISCARD"]

unwatch :: Monad m => Redis e m (e (Result ()))
unwatch = singleton $ Unwatch $ cmd 1 ["UNWATCH"]

watch :: Monad m => NonEmpty Key -> Redis e m (e (Result ()))
watch kk = singleton $ Watch $ cmd (1 + NE.length kk) $ "WATCH" : map key (toList kk)

exec :: (Monad m, FromByteString a) => Redis e m (e (Result [a]))
exec = singleton $ Exec $ cmd 1 ["EXEC"]

execRaw :: Monad m => Redis e m (e (Result Resp))
execRaw = singleton $ ExecRaw $ cmd 1 ["EXEC"]

-----------------------------------------------------------------------------
-- Keys

dump :: Monad m => Key -> Redis e m (e (Result (Maybe ByteString)))
dump k = singleton $ Dump $ cmd 2 ["DUMP", key k]

exists :: Monad m => Key -> Redis e m (e (Result Bool))
exists k = singleton $ Exists $ cmd 2 ["EXISTS", key k]

del :: Monad m => NonEmpty Key -> Redis e m (e (Result Int64))
del kk = singleton $ Del $ cmd (1 + NE.length kk) $ "DEL" : map key (toList kk)

expire :: Monad m => Key -> Seconds -> Redis e m (e (Result Bool))
expire k (Seconds n) = singleton $ Expire $ cmd 3 ["EXPIRE", key k, int2bytes n]

expireat :: Monad m => Key -> Timestamp -> Redis e m (e (Result Bool))
expireat k (Timestamp n) = singleton $ ExpireAt $ cmd 3 ["EXPIREAT", key k, int2bytes n]

persist :: Monad m => Key -> Redis e m (e (Result Bool))
persist k = singleton $ Persist $ cmd 2 ["PERSIST", key k]

keys :: Monad m => ByteString -> Redis e m (e (Result [Key]))
keys pat = singleton $ Keys $ cmd 2 ["KEYS", pat]

randomkey :: Monad m => Redis e m (e (Result (Maybe Key)))
randomkey = singleton $ RandomKey $ cmd 1 ["RANDOMKEY"]

rename :: Monad m => Key -> Key -> Redis e m (e (Result ()))
rename a b = singleton $ Rename $ cmd 3 ["RENAME", key a, key b]

renamenx :: Monad m => Key -> Key -> Redis e m (e (Result Bool))
renamenx a b = singleton $ RenameNx $ cmd 3 ["RENAMENX", key a, key b]

ttl :: Monad m => Key -> Redis e m (e (Result (Maybe TTL)))
ttl k = singleton $ Ttl $ cmd 2 ["TTL", key k]

typeof :: Monad m => Key -> Redis e m (e (Result (Maybe RedisType)))
typeof k = singleton $ Type $ cmd 2 ["TYPE", key k]

-----------------------------------------------------------------------------
-- Strings

get :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result (Maybe a)))
get k = singleton $ Get $ cmd 2 ["GET", key k]

set :: Monad m => Key -> ByteString -> Opts "SET" -> Redis e m (e (Result Bool))
set k v o = singleton $ Set $ cmd (3 + len o) $ "SET" : key k : v : toList (opts o)

ex :: Int64 -> Opts "SET"
ex i = Opts 2 $ "EX" `cons` DL.singleton (int2bytes i)

px :: Int64 -> Opts "SET"
px i = Opts 2 $ "PX" `cons` DL.singleton (int2bytes i)

xx :: Opts "SET"
xx = Opts 1 $ DL.singleton "XX"

nx :: Opts "SET"
nx = Opts 1 $ DL.singleton "NX"

getset :: (Monad m, FromByteString a) => Key -> ByteString -> Redis e m (e (Result (Maybe a)))
getset k v = singleton $ GetSet $ cmd 3 ["GETSET", key k, v]

mget :: (Monad m, FromByteString a) => NonEmpty Key -> Redis e m (e (Result [Maybe a]))
mget kk = singleton $ MGet $ cmd (1 + NE.length kk) $ "MGET" : map key (toList kk)

mset :: Monad m => NonEmpty (Key, ByteString) -> Redis e m (e (Result ()))
mset kv = singleton $ MSet $ cmd (1 + 2 * NE.length kv) $ "MSET" : foldr f [] kv
  where
    f (k, v) acc = key k : v : acc

msetnx :: Monad m => NonEmpty (Key, ByteString) -> Redis e m (e (Result Bool))
msetnx kv = singleton $ MSetNx $ cmd (1 + 2 * NE.length kv) $ "MSETNX" : foldr f [] kv
  where
    f (k, v) acc = key k : v : acc

getrange :: (Monad m, FromByteString a) => Key -> Int64 -> Int64 -> Redis e m (e (Result a))
getrange k a b = singleton $ GetRange $ cmd 4 ["GETRANGE", key k, int2bytes a, int2bytes b]

setrange :: Monad m => Key -> Int64 -> ByteString -> Redis e m (e (Result Int64))
setrange k i a = singleton $ SetRange $ cmd 4 ["SETRANGE", key k, int2bytes i, a]

append :: Monad m => Key -> ByteString -> Redis e m (e (Result Int64))
append k v = singleton $ Append $ cmd 3 ["APPEND", key k, v]

strlen :: Monad m => Key -> Redis e m (e (Result Int64))
strlen k = singleton $ StrLen $ cmd 2 ["STRLEN", key k]

decr :: Monad m => Key -> Redis e m (e (Result Int64))
decr k = singleton $ Decr $ cmd 2 ["DECR", key k]

decrby :: Monad m => Key -> Int64 -> Redis e m (e (Result Int64))
decrby k v = singleton $ DecrBy $ cmd 3 ["DECRBY", key k, int2bytes v]

incr :: Monad m => Key -> Redis e m (e (Result Int64))
incr k = singleton $ Incr $ cmd 2 ["INCR", key k]

incrby :: Monad m => Key -> Int64 -> Redis e m (e (Result Int64))
incrby k v = singleton $ IncrBy $ cmd 3 ["INCRBY", key k, int2bytes v]

incrbyfloat :: Monad m => Key -> Double -> Redis e m (e (Result Double))
incrbyfloat k v = singleton $ IncrByFloat $ cmd 3 ["INCRBYFLOAT", key k, dbl2bytes v]

-----------------------------------------------------------------------------
-- Bits

bitcount :: Monad m => Key -> Opts "RANGE" -> Redis e m (e (Result Int64))
bitcount k o = singleton $ BitCount $ cmd (2 + len o) $ "BITCOUNT" : key k : toList (opts o)

range :: Int64 -> Int64 -> Opts "RANGE"
range x y = Opts 2 $ int2bytes x `cons` DL.singleton (int2bytes y)

bitand :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result Int64))
bitand k kk = singleton $ BitAnd $ bitop "AND" (k <| kk)

bitor :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result Int64))
bitor k kk = singleton $ BitOr $ bitop "OR" (k <| kk)

bitxor :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result Int64))
bitxor k kk = singleton $ BitXOr $ bitop "XOR" (k <| kk)

bitnot :: Monad m => Key -> Key -> Redis e m (e (Result Int64))
bitnot k l = singleton $ BitNot $ bitop "NOT" (k :| [l])

bitop :: ByteString -> NonEmpty Key -> Resp
bitop o kk = cmd (2 + NE.length kk) $ "BITOP" : o : map key (toList kk)
{-# INLINE bitop #-}

bitpos :: Monad m => Key -> Bool -> BitStart -> BitEnd -> Redis e m (e (Result Int64))
bitpos k b (BitStart s) (BitEnd e) =
    let args = filter (not . B.null) [s, e] in
    singleton $ BitPos $ cmd (3 + length args) $ "BITPOS" : key k : toBit b : args
  where
    toBit True  = "1"
    toBit False = "0"

getbit :: Monad m => Key -> Int64 -> Redis e m (e (Result Int64))
getbit k o = singleton $ GetBit $ cmd 3 ["GETBIT", key k, int2bytes o]

setbit :: Monad m => Key -> Int64 -> Bool -> Redis e m (e (Result Int64))
setbit k o b = singleton $ SetBit $ cmd 4 ["SETBIT", key k, int2bytes o, toBit b]
  where
    toBit True  = "1"
    toBit False = "0"

-----------------------------------------------------------------------------
-- Hashes

hget :: (Monad m, FromByteString a) => Key -> Field -> Redis e m (e (Result (Maybe a)))
hget h k = singleton $ HGet $ cmd 3 ["HGET", key h, k]

hgetall :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result [(Field, a)]))
hgetall h = singleton $ HGetAll $ cmd 2 ["HGETALL", key h]

hvals :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result [a]))
hvals h = singleton $ HVals $ cmd 2 ["HVALS", key h]

hmget :: (Monad m, FromByteString a) => Key -> NonEmpty Field -> Redis e m (e (Result [Maybe a]))
hmget h kk = singleton $ HMGet $ cmd (2 + NE.length kk) $ "HMGET" : key h : toList kk

hmset :: Monad m => Key -> NonEmpty (Field, ByteString) -> Redis e m (e (Result ()))
hmset h kv = singleton $ HMSet $ cmd (2 + 2 * NE.length kv) $ "HMSET" : key h : foldr f [] kv
  where
    f (k, v) acc = k : v : acc

hset :: Monad m => Key -> Field -> ByteString -> Redis e m (e (Result Bool))
hset h k v = singleton $ HSet $ cmd 4 ["HSET", key h, k, v]

hsetnx :: Monad m => Key -> Field -> ByteString -> Redis e m (e (Result Bool))
hsetnx h k v = singleton $ HSetNx $ cmd 4 ["HSETNX", key h, k, v]

hincrby :: Monad m => Key -> Field -> Int64 -> Redis e m (e (Result Int64))
hincrby h k v = singleton $ HIncrBy $ cmd 4 ["HINCRBY", key h, k, int2bytes v]

hincrbyfloat :: Monad m => Key -> Field -> Double -> Redis e m (e (Result Double))
hincrbyfloat h k v = singleton $ HIncrByFloat $ cmd 4 ["HINCRBYFLOAT", key h, k, dbl2bytes v]

hdel :: Monad m => Key -> NonEmpty Field -> Redis e m (e (Result Int64))
hdel h kk = singleton $ HDel $ cmd (2 + NE.length kk) $ "HDEL" : key h : toList kk

hexists :: Monad m => Key -> Field -> Redis e m (e (Result Bool))
hexists h k = singleton $ HExists $ cmd 3 ["HEXISTS", key h, k]

hkeys :: Monad m => Key -> Redis e m (e (Result [Field]))
hkeys h = singleton $ HKeys $ cmd 2 ["HKEYS", key h]

hlen :: Monad m => Key -> Redis e m (e (Result Int64))
hlen h = singleton $ HLen $ cmd 2 ["HLEN", key h]

-----------------------------------------------------------------------------
-- Lists

lindex :: (Monad m, FromByteString a) => Key -> Index -> Redis e m (e (Result (Maybe a)))
lindex k i = singleton $ LIndex $ cmd 3 ["LINDEX", key k, int2bytes i]

lpop :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result (Maybe a)))
lpop k = singleton $ LPop $ cmd 2 ["LPOP", key k]

rpop :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result (Maybe a)))
rpop k = singleton $ RPop $ cmd 2 ["RPOP", key k]

rpoplpush :: (Monad m, FromByteString a) => Key -> Key -> Redis e m (e (Result (Maybe a)))
rpoplpush a b = singleton $ RPopLPush $ cmd 3 ["RPOPLPUSH", key a, key b]

brpoplpush :: (Monad m, FromByteString a) => Key -> Key -> Seconds -> Redis e m (e (Result (Maybe a)))
brpoplpush a b (Seconds t) = singleton $ BRPopLPush t $ cmd 4 ["BRPOPLPUSH", key a, key b, int2bytes t]

blpop :: (Monad m, FromByteString a) => NonEmpty Key -> Seconds -> Redis e m (e (Result (Maybe (Key, a))))
blpop kk (Seconds t) = singleton $ BLPop t $ cmd (2 + NE.length kk) $
    "BLPOP" : map key (toList kk) ++ [int2bytes t]

brpop :: (Monad m, FromByteString a) => NonEmpty Key -> Seconds -> Redis e m (e (Result (Maybe (Key, a))))
brpop kk (Seconds t) = singleton $ BRPop t $ cmd (2 + NE.length kk) $
    "BRPOP" : map key (toList kk) ++ [int2bytes t]

lrange :: (Monad m, FromByteString a) => Key -> Int64 -> Int64 -> Redis e m (e (Result [a]))
lrange k a b = singleton $ LRange $ cmd 4 ["LRANGE", key k, int2bytes a, int2bytes b]

linsert :: Monad m => Key -> Side -> ByteString -> ByteString -> Redis e m (e (Result Int64))
linsert k s p v = singleton $ LInsert $ cmd 5 ["LINSERT", key k, side2bytes s, p, v]

lpush :: Monad m => Key -> NonEmpty ByteString -> Redis e m (e (Result Int64))
lpush k vv = singleton $ LPush $ cmd (2 + NE.length vv) $ "LPUSH" : key k : toList vv

rpush :: Monad m => Key -> NonEmpty ByteString -> Redis e m (e (Result Int64))
rpush k vv = singleton $ RPush $ cmd (2 + NE.length vv) $ "RPUSH" : key k : toList vv

lset :: Monad m => Key -> Int64 -> ByteString -> Redis e m (e (Result ()))
lset k i v = singleton $ LSet $ cmd 4 ["LSET", key k, int2bytes i, v]

ltrim :: Monad m => Key -> Int64 -> Int64 -> Redis e m (e (Result ()))
ltrim k i j = singleton $ LTrim $ cmd 4 ["LTRIM", key k, int2bytes i, int2bytes j]

lrem :: Monad m => Key -> Int64 -> ByteString -> Redis e m (e (Result Int64))
lrem k c v = singleton $ LRem $ cmd 4 ["LREM", key k, int2bytes c, v]

lpushx :: Monad m => Key -> ByteString -> Redis e m (e (Result Int64))
lpushx k v = singleton $ LPushX $ cmd 3 ["LPUSHX", key k, v]

rpushx :: Monad m => Key -> ByteString -> Redis e m (e (Result Int64))
rpushx k v = singleton $ RPushX $ cmd 3 ["RPUSHX", key k, v]

llen :: Monad m => Key -> Redis e m (e (Result Int64))
llen k = singleton $ LLen $ cmd 2 ["LLEN", key k]

-----------------------------------------------------------------------------
-- Sets

spop :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result (Maybe a)))
spop k = singleton $ SPop $ cmd 2 ["SPOP", key k]

sismember :: Monad m => Key -> ByteString -> Redis e m (e (Result Bool))
sismember k v = singleton $ SIsMember $ cmd 3 ["SISMEMBER", key k, v]

smove :: Monad m => Key -> Key -> ByteString -> Redis e m (e (Result Bool))
smove a b v = singleton $ SMove $ cmd 4 ["SMOVE", key a, key b, v]

sdiff :: (Monad m, FromByteString a) => NonEmpty Key -> Redis e m (e (Result [a]))
sdiff kk = singleton $ SDiff $ cmd (1 + NE.length kk) $ "SDIFF" : map key (toList kk)

sinter :: (Monad m, FromByteString a) => NonEmpty Key -> Redis e m (e (Result [a]))
sinter kk = singleton $ SInter $ cmd (1 + NE.length kk) $ "SINTER" : map key (toList kk)

smembers :: (Monad m, FromByteString a) => Key -> Redis e m (e (Result [a]))
smembers k = singleton $ SMembers $ cmd 2 ["SMEMBERS", key k]

srandmember :: (Monad m, FromByteString a) => Key -> Choose -> Redis e m (e (Result [a]))
srandmember k One = singleton $ SRandMember One $ cmd 2 ["SRANDMEMBER", key k]
srandmember k chs = singleton $ SRandMember chs $ cmd 3 ["SRANDMEMBER", key k, choose chs]
  where
    choose (Dist n) = int2bytes n
    choose (Arb  n) = int2bytes n
    choose One      = "1"

sdiffstore :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result Int64))
sdiffstore k kk = singleton $ SDiffStore $ cmd (2 + NE.length kk) $
    "SDIFFSTORE" : key k : map key (toList kk)

sinterstore :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result Int64))
sinterstore k kk = singleton $ SInterStore $ cmd (2 + NE.length kk) $
    "SINTERSTORE" : key k : map key (toList kk)

sunionstore :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result Int64))
sunionstore k kk = singleton $ SUnionStore $ cmd (2 + NE.length kk) $
    "SUNIONSTORE" : key k : map key (toList kk)

sadd :: Monad m => Key -> NonEmpty ByteString -> Redis e m (e (Result Int64))
sadd k v = singleton $ SAdd $ cmd (2 + NE.length v) $ "SADD" : key k : toList v

srem :: Monad m => Key -> NonEmpty ByteString -> Redis e m (e (Result Int64))
srem k vv = singleton $ SRem $ cmd (2 + NE.length vv) $ "SREM" : key k : toList vv

sunion :: (Monad m, FromByteString a) => NonEmpty Key -> Redis e m (e (Result [a]))
sunion kk = singleton $ SUnion $ cmd (1 + NE.length kk) $ "SUNION" : map key (toList kk)

scard :: Monad m => Key -> Redis e m (e (Result Int64))
scard k = singleton $ SCard $ cmd 2 ["SCARD", key k]

-----------------------------------------------------------------------------
-- Sorted Sets

zrange :: (Monad m, FromByteString a) => Key -> Int64 -> Int64 -> Bool -> Redis e m (e (Result (ScoreList a)))
zrange k a b s =
    let args = ["ZRANGE", key k, int2bytes a, int2bytes b, "WITHSCORES"]
    in if s then singleton $ ZRange s $ cmd 5 args
            else singleton $ ZRange s $ cmd 4 (init args)

zrevrange :: (Monad m, FromByteString a) => Key -> Int64 -> Int64 -> Bool -> Redis e m (e (Result (ScoreList a)))
zrevrange k a b s =
    let args = ["ZREVRANGE", key k, int2bytes a, int2bytes b, "WITHSCORES"]
    in if s then singleton $ ZRevRange s $ cmd 5 args
            else singleton $ ZRevRange s $ cmd 4 (init args)

zrangebyscore :: (Monad m, FromByteString a) => Key -> Double -> Double -> Bool -> Opts "LIMIT" -> Redis e m (e (Result (ScoreList a)))
zrangebyscore k a b s o =
    let args = ["ZRANGEBYSCORE", key k, dbl2bytes a, dbl2bytes b, "WITHSCORES"] in
    if s then singleton $ ZRangeByScore s $ cmd (5 + len o) $ args ++ toList (opts o)
         else singleton $ ZRangeByScore s $ cmd (4 + len o) $ init args ++ toList (opts o)

zrevrangebyscore :: (Monad m, FromByteString a) => Key -> Double -> Double -> Bool -> Opts "LIMIT" -> Redis e m (e (Result (ScoreList a)))
zrevrangebyscore k a b s o =
    let args = ["ZREVRANGEBYSCORE", key k, dbl2bytes a, dbl2bytes b, "WITHSCORES"] in
    if s then singleton $ ZRevRangeByScore s $ cmd (5 + len o) $ args ++ toList (opts o)
         else singleton $ ZRevRangeByScore s $ cmd (4 + len o) $ init args ++ toList (opts o)

zadd :: Monad m => Key -> NonEmpty (Double, ByteString) -> Redis e m (e (Result Int64))
zadd k v = singleton $ ZAdd $ cmd (2 + 2 * NE.length v) $ "ZADD" : key k : foldr f [] v
  where
    f (i, x) acc = dbl2bytes i : x : acc

zinterstore :: Monad m => Key -> NonEmpty Key -> [Int64] -> Aggregate -> Redis e m (e (Result Int64))
zinterstore = _interstore ZInterStore "ZINTERSTORE"

zunionstore :: Monad m => Key -> NonEmpty Key -> [Int64] -> Aggregate -> Redis e m (e (Result Int64))
zunionstore = _interstore ZUnionStore "ZUNIONSTORE"

_interstore :: (Resp -> Command e (e (Result Int64)))
            -> ByteString
            -> Key
            -> NonEmpty Key
            -> [Int64]
            -> Aggregate
            -> Redis e m (e (Result Int64))
_interstore c n k kk ww a =
    let ww'     = map int2bytes ww ++ repeat "1"
        aggr    = aggr2bytes a
        nkeys   = NE.length kk
        ntotal  = 3 + 2 * nkeys + 1 + length aggr
        keys'   = map key (toList kk)
        weights = "WEIGHTS" : take nkeys ww'
    in singleton $ c $ cmd ntotal $ toList $ n
        `cons` key k
        `cons` int2bytes (fromIntegral nkeys)
        `cons` DL.fromList keys'
        <>     DL.fromList weights
        <>     DL.fromList aggr
  where
    aggr2bytes :: Aggregate -> [ByteString]
    aggr2bytes None = []
    aggr2bytes Min  = ["AGGREGATE", "MIN"]
    aggr2bytes Max  = ["AGGREGATE", "MAX"]
    aggr2bytes Sum  = ["AGGREGATE", "SUM"]
{-# INLINE _interstore #-}

zlexcount :: Monad m => Key -> Min -> Max -> Redis e m (e (Result Int64))
zlexcount k a b = singleton $ ZLexCount $ cmd 4 ["ZLEXCOUNT", key k, min2bytes a, max2bytes b]

min2bytes :: Min -> ByteString
min2bytes MinInf      = "-"
min2bytes (MinIncl x) = "[" <> x
min2bytes (MinExcl x) = "(" <> x

max2bytes :: Max -> ByteString
max2bytes MaxInf      = "+"
max2bytes (MaxIncl x) = "[" <> x
max2bytes (MaxExcl x) = "(" <> x

zrangebylex :: (Monad m, FromByteString a) => Key -> Min -> Max -> Opts "LIMIT" -> Redis e m (e (Result [a]))
zrangebylex k a b o = singleton $ ZRangeByLex $ cmd (4 + len o) $
    "ZRANGEBYLEX" : key k : min2bytes a : max2bytes b : toList (opts o)

zremrangebylex :: Monad m => Key -> Min -> Max -> Redis e m (e (Result Int64))
zremrangebylex k a b = singleton $ ZRemRangeByLex $ cmd 4
    ["ZREMRANGEBYLEX", key k, min2bytes a, max2bytes b]

zremrangebyrank :: Monad m => Key -> Int64 -> Int64 -> Redis e m (e (Result Int64))
zremrangebyrank k a b = singleton $ ZRemRangeByRank $ cmd 4
    ["ZREMRANGEBYRANK", key k, int2bytes a, int2bytes b]

zremrangebyscore :: Monad m => Key -> Double -> Double -> Redis e m (e (Result Int64))
zremrangebyscore k a b = singleton $ ZRemRangeByScore $ cmd 4
    ["ZREMRANGEBYSCORE", key k, dbl2bytes a, dbl2bytes b]

zrem :: Monad m => Key -> NonEmpty ByteString -> Redis e m (e (Result Int64))
zrem k vv = singleton $ ZRem $ cmd (2 + NE.length vv) $ "ZREM" : key k : toList vv

zincrby :: Monad m => Key -> Double -> ByteString -> Redis e m (e (Result Double))
zincrby k i v = singleton $ ZIncrBy $ cmd 4 ["ZINCRBY", key k, dbl2bytes i, v]

zscore :: Monad m => Key -> ByteString -> Redis e m (e (Result (Maybe Double)))
zscore k v = singleton $ ZScore $ cmd 3 ["ZSCORE", key k, v]

zcard :: Monad m => Key -> Redis e m (e (Result Int64))
zcard k = singleton $ ZCard $ cmd 2 ["ZCARD", key k]

zcount :: Monad m => Key -> Double -> Double -> Redis e m (e (Result Int64))
zcount k a b = singleton $ ZCount $ cmd 4 ["ZCOUNT", key k, dbl2bytes a, dbl2bytes b]

zrank :: Monad m => Key -> ByteString -> Redis e m (e (Result (Maybe Int64)))
zrank k a = singleton $ ZRank $ cmd 3 ["ZRANK", key k, a]

zrevrank :: Monad m => Key -> ByteString -> Redis e m (e (Result (Maybe Int64)))
zrevrank k a = singleton $ ZRevRank $ cmd 3 ["ZREVRANK", key k, a]

-----------------------------------------------------------------------------
-- HyperLogLog

pfadd :: Monad m => Key -> NonEmpty ByteString -> Redis e m (e (Result Bool))
pfadd k v = singleton $ PfAdd $ cmd (2 + NE.length v) $ "PFADD" : key k : toList v

pfcount :: Monad m => NonEmpty Key -> Redis e m (e (Result Int64))
pfcount kk = singleton $ PfCount $ cmd (1 + NE.length kk) $ "PFCOUNT" : map key (toList kk)

pfmerge :: Monad m => Key -> NonEmpty Key -> Redis e m (e (Result ()))
pfmerge k kk = singleton $ PfMerge $ cmd (2 + NE.length kk) $ "PFMERGE" : key k : map key (toList kk)

-----------------------------------------------------------------------------
-- Scan

scan :: (Monad m, FromByteString a) => Cursor -> Opts "SCAN" -> Redis e m (e (Result (Cursor, [a])))
scan c o = singleton $ Scan $ cmd (2 + len o) $ "SCAN" : cursor c : toList (opts o)

hscan :: (Monad m, FromByteString a) => Key -> Cursor -> Opts "SCAN" -> Redis e m (e (Result (Cursor, [a])))
hscan h c o = singleton $ HScan $ cmd (3 + len o) $ "HSCAN" : key h : cursor c : toList (opts o)

sscan :: (Monad m, FromByteString a) => Key -> Cursor -> Opts "SCAN" -> Redis e m (e (Result (Cursor, [a])))
sscan h c o = singleton $ SScan $ cmd (3 + len o) $ "SSCAN" : key h : cursor c : toList (opts o)

zscan :: (Monad m, FromByteString a) => Key -> Cursor -> Opts "SCAN" -> Redis e m (e (Result (Cursor, [a])))
zscan h c o = singleton $ ZScan $ cmd (3 + len o) $ "ZSCAN" : key h : cursor c : toList (opts o)

match :: ByteString -> Opts "SCAN"
match pat = Opts 2 $ "MATCH" `cons` DL.singleton pat

count :: Int64 -> Opts "SCAN"
count n = Opts 2 $ "COUNT" `cons` DL.singleton (int2bytes n)

-----------------------------------------------------------------------------
-- Sort

sort :: (Monad m, FromByteString a) => Key -> Opts "SORT" -> Redis e m (e (Result [a]))
sort k o = singleton $ Sort $ cmd (2 + len o) $ "SORT" : key k : toList (opts o)

by :: ByteString -> Opts "SORT"
by pat = Opts 2 $ "BY" `cons` DL.singleton pat

limit :: Int64 -> Int64 -> Opts o
limit off cnt = Opts 3 $ DL.fromList ["LIMIT", int2bytes off, int2bytes cnt]

getkey :: NonEmpty ByteString -> Opts "SORT"
getkey pats = foldr f none (toList pats)
  where
    f p acc = Opts 2 ("GET" `cons` DL.singleton p) <> acc

asc :: Opts "SORT"
asc = Opts 1 $ DL.singleton "ASC"

desc :: Opts "SORT"
desc = Opts 1 $ DL.singleton "DESC"

alpha :: Opts "SORT"
alpha = Opts 1 $ DL.singleton "ALPHA"

store :: Key -> Opts "SORT"
store k = Opts 2 $ "STORE" `cons` DL.singleton (key k)

-----------------------------------------------------------------------------
-- Pub/Sub

publish :: Monad m => ByteString -> ByteString -> Redis e m (e (Result Int64))
publish c m = singleton $ Publish $ cmd 3 $ ["PUBLISH", c, m]

subscribe :: Monad m => NonEmpty ByteString -> PubSub m ()
subscribe cs = singleton $ Subscribe $ cmd (1 + NE.length cs) $ "SUBSCRIBE" : toList cs

psubscribe :: Monad m => NonEmpty ByteString -> PubSub m ()
psubscribe cs = singleton $ PSubscribe $ cmd (1 + NE.length cs) $ "PSUBSCRIBE" : toList cs

unsubscribe :: Monad m => [ByteString] -> PubSub m ()
unsubscribe cs = singleton $ Unsubscribe $ cmd (1 + length cs) $ "UNSUBSCRIBE" : toList cs

punsubscribe :: Monad m => [ByteString] -> PubSub m ()
punsubscribe cs = singleton $ PUnsubscribe $ cmd (1 + length cs) $ "PUNSUBSCRIBE" : toList cs

-----------------------------------------------------------------------------
-- Responses

readInt'Null :: String -> Resp -> Result (Maybe Int64)
readInt'Null _ (Int i)  = Right $ Just i
readInt'Null _ NullBulk = Right Nothing
readInt'Null s _        = Left $ InvalidResponse s

readInt :: String -> Resp -> Result Int64
readInt _ (Int i) = Right i
readInt s _       = Left $ InvalidResponse s

readTTL :: String -> Resp -> Result (Maybe TTL)
readTTL s r = toTTL <$> readInt s r
  where
    toTTL (-2) = Nothing
    toTTL (-1) = Just NoTTL
    toTTL n    = Just $ TTL n

readBool :: String -> Resp -> Result Bool
readBool s r = readInt s r >>= toBool
  where
    toBool 0 = Right False
    toBool 1 = Right True
    toBool _ = Left $ InvalidResponse s

readListOfMaybes :: FromByteString a => String -> Resp -> Result [Maybe a]
readListOfMaybes n (Array _ r) = foldr f (Right []) r
  where
    f _        x@(Left _)  = x
    f NullBulk (Right acc) = Right $ Nothing : acc
    f (Bulk s) (Right acc) = (:acc) . Just <$> readStr s
    f (Str  s) (Right acc) = (:acc) . Just <$> readStr s
    f _        _           = Left $ InvalidResponse n
readListOfMaybes n _ = Left $ InvalidResponse n

readList :: FromByteString a => String -> Resp -> Result [a]
readList r (Array _ v) = foldr f (Right []) v
  where
    f _        x@(Left _)  = x
    f (Bulk s) (Right acc) = (:acc) <$> readStr s
    f (Str  s) (Right acc) = (:acc) <$> readStr s
    f _        _           = Left $ InvalidResponse r
readList r _ = Left $ InvalidResponse r

readScoreList :: FromByteString a => String -> Bool -> Resp -> Result (ScoreList a)
readScoreList r False a           = ScoreList [] <$> readList r a
readScoreList r True  (Array _ v) = toScoreList . unzip <$> foldr f (Right []) (chunksOf 2 v)
  where
    f _                x@(Left _)  = x
    f [Bulk x, Bulk d] (Right acc) = (\a b -> (a, b):acc) <$> readStr x <*> readStr d
    f _        _                   = Left $ InvalidResponse r
    toScoreList (a, s) = ScoreList s a
readScoreList r _ _ = Left $ InvalidResponse r

readFields :: FromByteString a => String -> Resp -> Result [(Field, a)]
readFields r (Array _ v) = foldr f (Right []) (chunksOf 2 v)
  where
    f _                x@(Left _)  = x
    f [Bulk k, Bulk x] (Right acc) = (:acc) . (k,) <$> readStr x
    f _                _           = Left $ InvalidResponse r
readFields r _ = Left $ InvalidResponse r

readKeyValue :: FromByteString a => String -> Resp -> Result (Maybe (Key, a))
readKeyValue _ NullArray                  = return Nothing
readKeyValue _ (Array _ [Bulk k, Bulk v]) = Just . (Key k,) <$> readStr v
readKeyValue r _                          = Left $ InvalidResponse r

readBulk'Null :: FromByteString a => String -> Resp -> Result (Maybe a)
readBulk'Null _ NullBulk = Right Nothing
readBulk'Null _ (Bulk s) = Just <$> readStr s
readBulk'Null n _        = Left $ InvalidResponse n

readBulk :: FromByteString a => String -> Resp -> Result a
readBulk _ (Bulk s) = readStr s
readBulk n _        = Left $ InvalidResponse n

readBulk'Array :: FromByteString a => String -> Choose -> Resp -> Result [a]
readBulk'Array n One r = maybeToList <$> readBulk'Null n r
readBulk'Array n _   r = readList n r

readScan :: FromByteString a => String -> Resp -> Result (Cursor, [a])
readScan n (Array 2 (Bulk c:v:[])) = (Cursor c,) <$> readList n v
readScan n _ = Left $ InvalidResponse n

matchStr :: String -> ByteString -> Resp -> Result ()
matchStr n x (Str s)
    | x == s    = Right ()
    | otherwise = Left $ InvalidResponse n
matchStr n _ _ = Left $ InvalidResponse n

anyStr :: String -> Resp -> Result ()
anyStr _ (Str _) = Right ()
anyStr n _       = Left $ InvalidResponse n

readType :: String -> Resp -> Result (Maybe RedisType)
readType _ (Str s) = case s of
    "string" -> return $ Just RedisString
    "hash"   -> return $ Just RedisHash
    "list"   -> return $ Just RedisList
    "set"    -> return $ Just RedisSet
    "zset"   -> return $ Just RedisZSet
    "none"   -> return Nothing
    _        -> Left $ InvalidConversion ("unknown redis type: " ++ show s)
readType n _ = Left $ InvalidResponse n

fromSet :: Resp -> Result Bool
fromSet (Str "OK") = Right True
fromSet NullBulk   = Right False
fromSet _          = Left $ InvalidResponse "SET"

readPushMessage :: Resp -> Result PushMessage
readPushMessage (Array 3 (Bulk "message":Bulk c:Bulk m:[])) =
    Right $ Message c m
readPushMessage (Array 4 (Bulk "pmessage":Bulk p:Bulk c:Bulk m:[])) =
    Right $ PMessage p c m
readPushMessage (Array 3 (Bulk "subscribe":Bulk c:Int n:[])) =
    Right $ SubscribeMessage c n
readPushMessage (Array 3 (Bulk "unsubscribe":Bulk c:Int n:[])) =
    Right $ UnsubscribeMessage c n
readPushMessage (Array 3 (Bulk "psubscribe":Bulk c:Int n:[])) =
    Right $ SubscribeMessage c n
readPushMessage (Array 3 (Bulk "punsubscribe":Bulk c:Int n:[])) =
    Right $ UnsubscribeMessage c n
readPushMessage _ = Left $ InvalidResponse "pub/sub"

-----------------------------------------------------------------------------
-- Helpers

cmd :: Int -> [ByteString] -> Resp
cmd n a = Array n (map Bulk a)
{-# INLINE cmd #-}

int2bytes :: Int64 -> ByteString
int2bytes = toStrict . toLazyByteStringWith (safeStrategy 20 8) mempty . int64Dec
{-# INLINE int2bytes #-}

dbl2bytes :: Double -> ByteString
dbl2bytes = toShortest
{-# INLINE dbl2bytes #-}

side2bytes :: Side -> ByteString
side2bytes Before = "BEFORE"
side2bytes After  = "AFTER"
{-# INLINE side2bytes #-}

readStr :: FromByteString a => ByteString -> Result a
readStr s = either (Left . InvalidConversion) Right $ runParser parser s
{-# INLINE readStr #-}
