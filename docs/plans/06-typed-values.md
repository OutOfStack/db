# Plan 06 — Typed Values (bool, int, float, string, array, map)

## Description

Values gain types beyond string: `bool`, `int`, `float`, `string`, `array`, `map` — plus the
server-side operations that justify them (`INCR`, array push, map field set/get). Without such
operations typed storage adds no value over clients storing JSON strings; with them, the server
eliminates racy client-side read-modify-write cycles.

## Relationship to other plans

- **Depends on plan 02 (Protocol framing):** typed payloads are arbitrary bytes; the
  length-prefixed argument encoding carries them without escaping. The value serialization
  defined here rides inside one protocol argument.
- **Depends on plan 03 (WAL persistence)** only in that WAL records store the serialized typed
  value transparently — no WAL format change, but recovery tests must cover typed payloads.
- **Independent of plan 04 (Replication):** replication ships opaque records; typed values
  replicate with zero changes.
- **Independent of plan 05 (LRU tiering):** the tiered engine stores serialized bytes; only its
  byte-accounting sees value sizes. The two can be built in parallel after plans 02–03.

## Design decisions

- **Wire & storage representation:** one self-describing binary encoding used everywhere
  (client↔server, engine, WAL, snapshots): 1 type tag byte + payload
  (varint for int, IEEE 754 for float, element count + recursive elements for array/map;
  map keys are strings). Implemented in `internal/protocol/value.go` — same package as framing,
  one serialization home. JSON was considered and rejected for the internal format (slower,
  loses int/float distinction); the CLI handles human-readable conversion instead.
- **Client-facing syntax (CLI):** typed literals — `SET t k 42` (int), `42.5` (float),
  `true`/`false` (bool), `"quoted"` (string), `[1,2,3]` (array), `{"a":1}` (map); bare words
  default to string. The CLI parses literals into typed values; programmatic clients construct
  them directly via `internal/client`.
- **Engine stores `Value`, not `string`:** `map[table]map[key]Value` where `Value` is the decoded
  typed representation (`internal/protocol`). `GET` returns the value with its type; the CLI
  renders it back as a literal.
- **New commands (the actual payoff), all atomic under the engine lock:**
  - `INCR <table> <key> [delta]` — int/float; creates as `0` if missing; type error otherwise.
  - `APPEND <table> <key> <value>` — push to array (creates array if missing).
  - `HSET <table> <key> <field> <value>` / `HGET <table> <key> <field>` — map field ops.
  - `TYPE <table> <key>` — returns the type name.
  Each is one WAL record; replay reproduces the same state (they are deterministic mutations).
- **Type errors are first-class:** `ERR wrong type: key holds array, INCR requires int` — modeled
  on Redis's `WRONGTYPE`.

## Implementation steps

1. **Value codec** (`internal/protocol/value.go`): `Value` type, encode/decode, deep-equal
   helper for tests, literal parser/renderer for the CLI (quoted strings, arrays, maps).
2. **Engine** (`internal/engine/`): store `Value`; add atomic `Incr`, `Append`, `HSet`, `HGet`
   operations behind the mutex. Mirror in the tiered engine (plan 05) if it exists — there the
   ops are read-decode-modify-encode-append under lock.
3. **Parser/Compute/Storage:** register new commands + arity; `Execute` dispatches them;
   regenerate mocks.
4. **WAL (plan 03):** no format change (records carry protocol-encoded args), but new record
   types for the new commands; recovery tests extended with typed payloads.
5. **CLI** (`cmd/db-cli`, `internal/client`): literal parsing on input, typed rendering on output.
6. **Migration:** existing string-only WAL/snapshot records (plans 03/05 data) are read as
   type-tag `string` via a version byte in the value encoding — old data keeps working.

## Testing

- Codec round-trip + fuzz tests for every type, including nesting (`array` of `map`s) and
  boundary ints/floats.
- Atomicity: N concurrent `INCR`s produce exactly N total delta (race detector on).
- Type-error matrix: every command against every wrong-type value.
- WAL replay equivalence with mixed typed operations; migration test loading a pre-plan-06 log.

## Out of scope

- Rich per-type command families (sorted sets, list ranges, set operations) — the four commands
  above prove the model; more are incremental parser+engine work.
- Secondary indexes or queries over values.
- Schema enforcement per table.
