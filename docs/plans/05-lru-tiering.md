# Plan 05 — Memory/Disk Tiering with LRU (RAM Limit < Storage Limit)

## Description

Let the dataset grow beyond RAM. Two configured limits: `max_memory` (hot data kept in RAM) and
`max_storage` (total dataset on disk). Disk is the source of truth; memory becomes an LRU cache
over it. When RAM is full, the least-recently-used entries are evicted from memory — eviction is
free and lossless because every value is already durable on disk. When storage is full, writes
are rejected with a clear error (no data is ever silently dropped).

The design is Bitcask-style: an append-only data store on disk with a full in-memory index
(table/key → file offset), plus an LRU cache of hot values.

## Relationship to other plans

- **Depends on plan 03 (WAL persistence):** "disk as source of truth" requires the WAL/snapshot
  infrastructure; this plan grows the plan-03 WAL segments into the permanent data store
  (records are no longer deleted after snapshotting — compaction replaces snapshot-truncation).
- **Independent of plan 04 (Replication):** tiering is node-local. A cluster can mix tiered and
  fully-in-memory nodes; replication ships WAL records either way. The two plans can be built in
  parallel after plan 03 lands.

## Design decisions

- **Keydir in memory, always:** a `map[(table,key)] → {segmentID, offset, size}` index of every
  live key. Memory limit applies to *cached values*, not the index (documented: ~100 bytes/key
  index overhead bounds total key count).
- **Read path:** LRU hit → return; miss → read value at offset from segment file, insert into
  LRU (evicting as needed), return.
- **Write path:** append to WAL (unchanged from plan 03) → update keydir → insert into LRU.
  `DEL` appends a tombstone and removes the keydir/LRU entries.
- **Compaction replaces snapshotting:** a background job rewrites the oldest segments, keeping
  only records that the keydir still points at, then atomically swaps keydir pointers and deletes
  the originals. Triggered by dead-bytes ratio. Startup recovery becomes: scan segments → rebuild
  keydir (an optional periodic keydir snapshot keeps restarts fast).
- **`max_storage` enforcement:** live-bytes accounting in the keydir; when live data + new record
  would exceed the limit, `SET` returns `ERR storage full`. Compaction may free space; eviction
  never does (eviction only touches RAM).
- **LRU implementation:** classic doubly-linked list + map guarded by the engine mutex; sized by
  approximate bytes (key + value + overhead), not entry count. No external dependency needed.
- **Engine selection stays config-driven:** `engine.type: in_memory` (plan 03 behavior, RAM-only)
  vs `engine.type: tiered`. The `storage.Engine` interface (`internal/storage/storage.go`) is
  unchanged — this is a new implementation behind it.

## Implementation steps

1. **New package** `internal/engine/tiered/`: keydir, LRU cache, segment reader; implements the
   existing `Set/Get/Del/Range` engine interface.
2. **WAL evolution** (`internal/wal/`): segments become the permanent store — add random-access
   reads by (segment, offset), record-level iteration for compaction, and retention driven by
   compaction instead of snapshot-truncation. Replication (plan 04), if present, is unaffected:
   it consumes records by LSN as before.
3. **Compactor:** background goroutine in `internal/engine/tiered/`; correctness rule — a record
   is live iff the keydir still points to its (segment, offset).
4. **Config** (`internal/config/server.go`): `engine.type: tiered`, `engine.max_memory`,
   `engine.max_storage`, compaction thresholds. Validate `max_memory <= max_storage`.
5. **Metrics/observability:** log cache hit rate, live vs dead bytes, compaction runs — needed
   to tune this at all.

## Testing

- Unit: LRU eviction order, byte accounting, keydir correctness across overwrite/delete.
- Property test: random workload against tiered engine and plain in-memory engine must produce
  identical GET results, with `max_memory` set tiny to force constant eviction.
- Compaction under concurrent writes (race detector); restart mid-compaction recovers cleanly.
- Storage-limit test: fill to `max_storage`, verify `ERR storage full`, delete keys, compact,
  verify writes resume.

## Out of scope

- Keydir spilling to disk (datasets whose *keys* don't fit in RAM).
- Per-table memory/storage quotas.
- Alternative caches (LFU, ARC) — LRU only, behind a small interface if swapping later.
