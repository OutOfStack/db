# Plan 01 — Tables (Namespaces)

## Description

Introduce tables as a first-class namespace so keys are scoped per table instead of living in
one global space. A key is now identified by `(table, key)`. Tables are created implicitly on
first write and removed implicitly when their last key is deleted (no `CREATE TABLE` / `DROP TABLE`
ceremony in this iteration).

This plan goes first because it changes the logical key format. Every later feature — WAL records
(plan 03), the replication stream (plan 04), cache keys in the LRU tier (plan 05) — will embed
this key format. Locking it in now avoids a painful log/protocol migration later.

## Relationship to other plans

- **Depends on:** nothing — this is the first plan.

## Design decisions

- **Table is part of every command, not session state.** Commands become
  `SET <table> <key> <value>`, `GET <table> <key>`, `DEL <table> <key>`.
  A `USE <table>` session command is explicitly rejected: connections in `pool.Client` are cached
  and reused across requests (`internal/pool/client.go`, `getConnection`), and with replication
  (plan 04) requests may land on different servers. Per-connection state would break both.
- **Implicit tables.** `SET` creates the table if missing; `DEL` of the last key may leave an
  empty table map — a cheap cleanup on `Del` keeps memory bounded.
- **Table name validation** in the parser: non-empty, no whitespace (the protocol is still
  whitespace-delimited until plan 02), max length (e.g. 128).

## Implementation steps

1. **Parser** (`internal/parser/parser.go`)
   - `SET` now requires 3 args, `GET`/`DEL` require 2. Update error messages.
   - Update `internal/parser/parser_test.go`.
2. **Engine** (`internal/engine/engine.go`)
   - Change `store map[string]string` → `store map[string]map[string]string` (table → key → value).
   - Method signatures gain a table argument: `Set(ctx, table, key, value)`, `Get(ctx, table, key)`,
     `Del(ctx, table, key)`. Missing table on `Get`/`Del` returns `ErrNotFound`.
   - Keep the single `sync.RWMutex`; per-table locking is premature at this scale.
3. **Storage** (`internal/storage/storage.go`)
   - Update the `Engine` interface and `Execute` to pass `args[0]` as table, `args[1]` as key,
     `args[2]` as value.
   - Regenerate mocks (`make generate`).
4. **Compute** (`internal/compute/compute.go`)
   - No structural change; fix the "Key not found" log line that assumes `args[0]` is the key.
5. **Docs & config samples** — update `CLAUDE.md` command examples and any sample configs / README.

## Testing

- Unit tests: same key in two tables is independent; `GET`/`DEL` on missing table → not found;
  table auto-created on `SET`; empty-table cleanup after last `DEL`.
- Run `make test` (race detection) and `make lint`.
- Manual smoke test with netcat: `echo "SET users name vlad" | nc localhost 3223`.

## Out of scope

- `CREATE/DROP/LIST TABLES` commands (can be added later as trivial parser + engine additions).
- Per-table configuration (limits, TTLs).