-- This Source Code Form is subject to the terms of the Mozilla Public
-- License, v. 2.0. If a copy of the MPL was not distributed with this
-- file, You can obtain one at http://mozilla.org/MPL/2.0/.

module Data.Redis
    ( Redis
    , Command (..)
    , Error   (..)
    , Key
    , Result
    , Opts

    , ping
    , get
    , set, ex, px, nx, xx

    , Pong (..)

    , Resp (..)
    , resp
    , decode
    , encode
    ) where

import Data.Redis.Resp
import Data.Redis.Command
