# REdis Serialisation Protocol (RESP)

[![Build Status](https://travis-ci.org/twittner/redis-resp.svg?branch=master)](https://travis-ci.org/twittner/redis-resp)

This library provides [RESP][1] encoding/decoding functionality and
defines most of the available Redis [commands][2] as an GADT which
enables different interpreter implementations such as [redis-io][3].

[1]: http://redis.io/topics/protocol
[2]: http://redis.io/commands
[3]: https://github.com/twittner/redis-io
