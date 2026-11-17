# Pebbis

An embedded, Redis-compatible data store written in Go, with [Pebble](https://github.com/cockroachdb/pebble) as its storage engine.

It can be used two ways:

- **As a library** — import it, open a `Store`, and talk RESP semantics without any socket. Persistence, expiration, eviction and transactions all work in-process.
- **As a server** — `cmd/pebbisd` speaks RESP2 over TCP, so any Redis client (`redis-cli`, go-redis, …) can connect.

## Design highlights

- **Pebble-backed durability.** Every key lives in an LSM tree; restarts replay nothing but Pebble's own WAL. `Checkpoint` provides consistent hard-link snapshots.
- **Skiplist-free in-memory index.** Aggregates (hash/list/set/zset/stream) are reconstructed lazily from the engine, bounded by cache size — memory stays proportional to the working set, not the dataset.
- **Eviction + TTL.** `maxmemory` with `noeviction` / `allkeys-lru` / `volatile-lru` / `allkeys-lfu` / `volatile-lfu` / `allkeys-random` / `volatile-random` / `volatile-ttl`, plus lazy + active key expiration. Above a 16 MiB budget the limit is enforced against the index *plus* Pebble's block cache and memtables (below it, only the index is counted), so the budget tracks the process footprint instead of just the key index.
- **Atomic transactions.** `MULTI`/`EXEC` takes the exclusive per-key locks of every key its queue touches and holds them for the whole execution, so another client cannot interleave a command between two queued commands; `WATCH` versions are verified under the same lock. The queued writes are buffered in one indexed Pebble batch and applied with a single atomic commit, so a crash either applies the whole transaction or none of it, and the queued reads see the buffered writes (counters, sparse aggregates and range scans included). Commands no per-key lock covers (`FLUSHALL`, `SWAPDB`, `CONFIG SET`, …) are still not isolated from concurrent clients, and a writing transaction holds the write gate while it runs, so single writes wait for it — the same visibility a single-threaded Redis gives.
- **Reliable streams.** Consumer groups with PEL: deliveries are recorded before they are sent, and acked entries are removed — pending entries survive restarts.
- **Safe defaults.** With no `requirepass` configured, protected mode refuses connections from non-loopback addresses (as Redis does), so starting on `:6379` does not expose the store to the network by accident.

## Quick start

Embedded (library API):

```go
cfg := pebbis.DefaultOptions()
cfg.Dir = "/path/to/data"
store, err := pebbis.Open(cfg)
if err != nil {
    return err
}
defer store.Close()
```

Embedded with a RESP endpoint — existing Redis clients switch by changing only the
connection string, and the store still lives inside your process:

```go
emb, err := pebbis.OpenEmbedded(pebbis.EmbedOptions{Dir: "/path/to/data"})
if err != nil {
    return err
}
defer emb.Close()
// Clients connect to emb.Addr(), a unix socket with mode 0600.
// Hot paths in this process can also call emb.Store() and skip the protocol.
```

`OpenEmbedded` spawns no child process: the listener is a goroutine over the same
`Store`, so a host restart restarts the store (Pebble replays its WAL) and there is
no orphan process to reap. The data directory is owned exclusively by one process.
Because clients still speak RESP, a command pays for parsing and a socket round trip —
embedded mode saves a separate deployment, not the protocol.

Server:

```
go build -o bin/pebbisd ./cmd/pebbisd
./bin/pebbisd -addr :6379 -dir ./data
```

Configuration (see `config/`): `maxmemory`, `maxmemory-policy`, `slowlog-log-slower-than`, `slowlog-max-len`, `lfu-decay-time`, `protected-mode`.

Protected mode is on by default: until `-requirepass` is set (or `-protected-mode=false`), connections from anything but loopback are refused.

## Supported commands (188)

- **Connection / server**: `PING`, `ECHO`, `SELECT`, `QUIT`, `HELLO`, `AUTH`, `COMMAND`, `CLIENT` (GET/GETNAME/ID/INFO/LIST/RESET/SETNAME), `CONFIG` (subset), `INFO`, `DEBUG`, `TIME`, `SORT`, `SORT_RO`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `SAVE`, `BGSAVE`, `LASTSAVE`, `SHUTDOWN`, `SLOWLOG`, `MEMORY`, `OBJECT` (subset)
- **Strings**: `GET`, `SET`, `SETNX`, `SETXX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `DELEX`, `MSET`, `MSETNX`, `MGET`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `GETRANGE`, `SETRANGE`, `SUBSTR`, `LCS`, `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`
- **Keys (generic)**: `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `EXPIRETIME`, `PEXPIRETIME`, `KEYS`, `SCAN`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `SWAPDB`, `TOUCH`
- **Hashes**: `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HKEYS`, `HVALS`, `HLEN`, `HSTRLEN`, `HINCRBY`, `HINCRBYFLOAT`, `HSCAN`, `HRANDFIELD`
- **Lists**: `LPUSH`, `LPUSHX`, `RPUSH`, `RPUSHX`, `LPOP`, `RPOP`, `LRANGE`, `LINDEX`, `LLEN`, `LREM`, `LSET`, `LTRIM`, `LINSERT`, `LMOVE`, `LPOS`, `LMPOP`, `RPOPLPUSH`, `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BLMOVE`, `BLMPOP`
- **Sets**: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SINTER`, `SINTERSTORE`, `SINTERCARD`, `SUNION`, `SUNIONSTORE`, `SDIFF`, `SDIFFSTORE`, `SSCAN`
- **Sorted sets**: `ZADD`, `ZCARD`, `ZCOUNT`, `ZINCRBY`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZREVRANGE`, `ZRANGE` (incl. `REV`/`BYSCORE`/`BYLEX`), `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZRANGESTORE`, `ZLEXCOUNT`, `ZREM`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZREMRANGEBYLEX`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `ZRANDMEMBER`, `ZUNION`, `ZUNIONSTORE`, `ZINTER`, `ZINTERSTORE`, `ZDIFF`, `ZDIFFSTORE`
- **Streams**: `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`, `XGROUP` (CREATE/DESTROY/CREATECONSUMER/DELCONSUMER), `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XINFO` (GROUPS/CONSUMERS/STREAM)
- **Pub/Sub**: `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`

## Unsupported commands

This is a cache/store, not a full Redis clone. Compared with Redis 7.x, the following are **not implemented**:

- **Clustering & replication**: `CLUSTER *`, `REPLICAOF`, `SLAVEOF`, `SYNC`, `PSYNC`, `WAIT`, `WAITAOF`, `FAILOVER`, `READONLY`, `READWRITE`
- **ACL**: the entire `ACL *` family (authentication via `AUTH <password>` works with a configured default password)
- **Bitmaps**: `BITFIELD`, `BITFIELD_RO` (the `SETBIT` / `GETBIT` / `BITCOUNT` / `BITPOS` / `BITOP` primitives are supported)
- **HyperLogLog**: `PFADD`, `PFCOUNT`, `PFMERGE`
- **Geospatial**: `GEOADD` and the whole `GEO*` family
- **Scripting / functions**: `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`, `SCRIPT *`, `FUNCTION *`
- **Server internals**: `MONITOR`, `LATENCY *`, `LOLWUT`, `COMMAND DOCS/INFO` (only the full `COMMAND` dump is served)
- **Data-command gaps**: `DUMP`, `RESTORE`, `MIGRATE`
- **Hash field TTL (Redis 7.4)**: `HGETEX`, `HGETDEL`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`, `HFIELDS`
- **Streams**: `XAUTOCLAIM`, `XSETID`
- **Keyspace notifications**: expired-key / keyevent channels are not published

Anything not listed here will return an `ERR unknown command` error, same as Redis.

## Production caveats

- **Single node.** No replication, cluster mode, failover, or `DUMP`/`RESTORE`/`MIGRATE`. The backup story is `Checkpoint` (hard-link snapshot) plus whatever copies it off the machine.
- **Durability is the configured one.** `-appendfsync group` (default) batches WAL fsyncs over `WALMinSyncInterval` (200 µs), so a power failure can lose the last acknowledged writes; use `-appendfsync always` when that matters.
- **No ACL, no scripting, no keyspace notifications** (see the unsupported list above). Authentication is a single `requirepass` password compared in plaintext — use TLS (`ServerOptions.TLSConfig`) when the network is not trusted.
- Treat it as an embedded store or a single-node cache backed by Pebble, not as the only copy of data you cannot rebuild.

## Requirements

Go 1.26 or newer. `go.mod` declares the language version only — no `toolchain` is pinned, so any newer Go release works without editing the module (the `go 1.26.0` line is Go's canonical form, not a pinned patch release). Tests that drive the server through go-redis pull `github.com/redis/go-redis/v9` from the module proxy. `make race` needs a working C toolchain (`CC`), since the race detector uses cgo.

## Development

```
make build   # build cmd/pebbisd into bin/
make test    # go test across all packages
make race    # go test -race across all packages (the standard gate)
make drill   # SIGKILL + restart drill: every acknowledged write must survive
make bench   # run benchmarks (XADD, sparse aggregates, ...)
make cover   # coverage profile + HTML report
```

CI (`.github/workflows/ci.yml`) runs build, vet, test, the race detector and the restart drill on every push and pull request.

`make drill` starts a real `pebbisd` with `-appendfsync always`, writes a mixed workload (strings including a value too large to be inline, a sparse hash, a list, a sorted set, a stream with un-acked consumer-group entries, counters, TTLs and a transaction), `SIGKILL`s the process without any graceful shutdown, restarts it on the same directory and verifies everything came back.

Layout: `storage/` (Pebble engine wrapper), `memory/` (aggregate encodings), `stream.go` (streams + consumer groups), `cmd_*.go` (one file per data type), `tests/compat/` (black-box RESP compatibility tests).
