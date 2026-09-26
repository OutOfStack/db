# Compatibility

This document is the v1 contract: what a v1.x release promises, and what is deliberately left outside the promise.
Anything not described here is an implementation detail and may change in any release.

Full Redis or RESP2 compatibility is **not** a goal. The subset documented below is the contract.

## The promise

Within the v1.x series there will be no incompatible change to:

- the **Go API** of the `client` package (the only importable package; everything else lives under `internal/`),
- the **wire protocol**: the command set, the reply shapes, and the error codes below,
- the **configuration** files and environment variables,
- the **storage formats**: WAL segments, snapshots, and tiered segments.

A format may still evolve, but only behind a compatible reader — a v1.x build reads everything earlier v1.x builds
wrote — or, where that is impossible, an offline migration tool shipped with the release that makes the change. A
release never requires a migration a user cannot run before starting the new binary.

Additive change is allowed and is not a break: new commands, new client methods, new configuration keys with
backward-compatible defaults, and new error codes for conditions that previously reported `ERR`.

Not covered by the promise:

- **Error message text.** Branch on the code, never on the message.
- **Log output**, metric names, and the contents of debug-level records.
- **Preview features**: the `tiered` engine, replication, and pooled failover. They are documented in the README's
  support boundary, and their behavior may change within v1.x.
- **Performance** characteristics and memory layout.

## Version identity

Both binaries print their identity, so a deployment can be checked against this document:

```bash
db -version
db-cli -version
```

```
release:  v1.0.0
commit:   0123456789abcdef0123456789abcdef01234567
protocol: RESP2
storage:  wal=2 snapshot=3 segment=2
go:       go1.27.1
```

`release` and `commit` are injected at build time; a build made without them reports `dev` and the revision the Go
toolchain recorded, so an unreleased binary never claims a version. The three storage numbers are the on-disk format
versions this build reads and writes — a file carrying any other version is refused at open rather than parsed.

## Wire protocol

RESP2 framing over TCP. A **request** is an array of bulk strings, one per token:

```
*4\r\n$3\r\nSET\r\n$5\r\nusers\r\n$4\r\nname\r\n$5\r\nAlice\r\n
```

A **reply** is one of five shapes:

| Shape | Encoding | Used for |
|-------|----------|----------|
| Simple string | `+OK\r\n` | write acknowledgements, `TYPE` |
| Bulk string | `$5\r\nAlice\r\n` | values |
| Null | `$-1\r\n` | a missing key or field |
| Integer | `:3\r\n` | `APPEND`'s new length |
| Array | `*2\r\n…` | `TABLES`, `KEYS` |
| Error | `-WRONGTYPE key holds string\r\n` | every failure |

Edges of the subset, all frozen:

- **Null arrays are outside the subset.** The server never emits `*-1\r\n`; an empty result is an empty array (`*0\r\n`),
  which is distinct from null. For robustness the client decodes a null array it might receive from some other server as
  the same null reply a null bulk string produces — it does not carry a separate type, because no reply can reach one.
- **Arguments are binary-safe.** RESP framing is length-prefixed, so an argument may contain spaces, CR, LF, and NUL.
  Only the CLI's line splitting constrains what is convenient to type.
- **Inline commands are not supported.** A request that is not an array of bulk strings is answered with a `PROTOCOL`
  error, after which the connection is closed.
- **The message-size limit applies per side, not per exchange.** A server rejects a *request* larger than its
  `max_message_size` with `TOOLARGE`; it does not bound the replies it writes, so a `KEYS` or `TABLES` listing can
  exceed that limit. What protects a client is its own configured limit: it refuses to decode a reply past it. That
  refusal is a local transport error rather than a coded `*ServerError` — nothing was wrong with the command, and the
  server has already written the bytes — and the client drops the connection and redials on the next call. Size a
  client's limit for the largest listing it means to read.

## Error codes

Every error reply begins with a code token, a space, and a human-readable message. The code is the stable part.

| Code | Meaning | Typical causes |
|------|---------|----------------|
| `ERR` | unclassified failure | anything without a narrower code |
| `PROTOCOL` | malformed request frame | not a RESP array, bad length prefix, missing CRLF |
| `UNKNOWNCMD` | no such command | a typo, or a command from a later version |
| `ARITY` | wrong number of arguments | `GET users` |
| `ARGUMENT` | an argument the command cannot interpret | empty table or key, unparsable value literal, non-numeric `INCR` delta |
| `TOOLARGE` | past a configured or format limit | table name over 128 bytes, message over `max_message_size`, tiered storage full |
| `WRONGTYPE` | the key holds another type, or the arithmetic does not fit it | `INCR` on a string, `HGET` on an array, `INCR` past the int64 range |
| `READONLY` | mutation sent to a replication standby | writes before a `PROMOTE` |
| `UNAVAILABLE` | the server cannot serve this in its current state | storage fenced or latched into a terminal state (a failed WAL or tiered fsync), replication not enabled, `PROMOTE` disabled |

A client that meets a code it does not recognize must treat it as `ERR`. Within v1.x a code is never removed and never
given a new meaning; a condition that reports `ERR` today may later be given a narrower code, which is why unrecognized
codes have to degrade rather than fail.

## Go client contract

The full contract is on the types in [`client`](client); this is the summary.

- **Unknown codes.** `ServerError.Code` is always one of the client's code constants: a code this client version does
  not recognize, from a later server, is reported as `CodeErr` with the server's token kept at the start of `Msg`.
- **Sentinels.** Only three conditions have exported sentinels, because only these three are usually acted on rather
  than reported: `ErrNotFound`, `ErrOutcomeUnknown`, and `ErrWrongType`. Everything else is a `*ServerError` — match it
  with `errors.As` and branch on `.Code`.
- **Concurrency.** A `Client` is safe for concurrent use by any number of goroutines, including concurrently with
  `Close`. Commands on one connection are serialized.
- **Contexts.** Every method's context bounds the whole call: waiting for a connection, dialling, writing, and reading
  the reply. `New` does not connect, so it reports configuration errors only.
- **Delivery.** A command is never re-sent once a complete frame may have reached a server. A mutation that fails after
  its frame went out returns `ErrOutcomeUnknown`; re-issuing it is the caller's decision. Reads, and failures proven to
  have happened before any byte was sent, are retried transparently.
- **Cancellation after transmission.** Cancelling there returns `ErrOutcomeUnknown` for a mutation, and the error also
  matches the context error — both `errors.Is(err, ErrOutcomeUnknown)` and `errors.Is(err, context.Canceled)` hold.
- **`Close`** is idempotent and terminal: safe to call more than once, interrupts commands in flight, and a closed
  client fails later calls rather than reconnecting.
- **`Set` takes a typed literal**, parsed server-side — `Set(ctx, t, k, "42")` stores the int 42, and `` `"42"` ``
  stores the string. It is not an unconditionally string-typed setter.
- **`Raw`** renders a reply as text: a simple or bulk string as itself, an integer as decimal digits, null as
  `not found`, and an array as its elements joined with `\n`. An error reply is returned as a `*ServerError` rather
  than rendered.

## Golden fixtures

[`internal/compat/testdata/golden/v1`](internal/compat/testdata/golden/v1) holds byte-for-byte fixtures generated at
v1.0: a WAL segment, a snapshot, and recorded request and reply streams covering every command and error shape. Tests in
that package run on every build and fail if this build would read or write any of them differently.

A v1.x release may add a fixture set for a newly frozen shape. It must never regenerate v1.0's — regenerating is how a
format change gets committed instead of caught, which is why the generator is a flag-guarded test rather than a tool.
