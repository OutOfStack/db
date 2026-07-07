# Plan 03 — Write-Ahead Log Persistence & Snapshots

## Description

Make data survive restarts. Every mutating command (`SET`, `DEL`) is appended to a write-ahead
log (WAL) on disk before the client gets an acknowledgment; on startup the engine is rebuilt by
loading the latest snapshot and replaying the WAL tail. Periodic snapshots plus log truncation
keep recovery time and disk usage bounded.

This is the keystone plan: the WAL is also the replication stream (plan 04) and makes disk the
source of truth so the LRU tier (plan 05) can treat memory as a pure cache.

## Relationship to other plans

- **Depends on plan 01 (Tables):** WAL records embed the `(table, key)` key format.
- **Depends on plan 02 (Protocol framing):** WAL records reuse `internal/protocol` length-prefixed
  encoding, so there is exactly one serialization format in the codebase.
- **Superseded in part by plan 05 (LRU tiering) — build snapshotting minimal.** Tiering is a
  confirmed goal, and plan 05 turns the WAL segments into the permanent data store: snapshotting
  + segment truncation from this plan is *replaced* by keydir-driven compaction. Everything else
  here (record format with LSNs, group commit, fsync policies, segmented files, crash recovery)
  carries over unchanged and is also the foundation for replication (plan 04). Therefore:
  implement the snapshot/truncation piece as simply as possible — single code path, no tuning,
  no incremental cleverness — and read plan 05's "WAL evolution" step before starting, so no
  effort goes into logic that plan 05 deletes.

## Design decisions

- **Record format:** `uint64` LSN (log sequence number, monotonic), record type (`SET`/`DEL`),
  then the protocol-encoded arguments, then a CRC32 checksum. LSNs become replication offsets in
  plan 04 — include them now.
- **Fsync policy is configurable** (`wal.sync` = `always` | `everysec` | `no`), mirroring Redis
  AOF semantics. Default `everysec`: a background ticker fsyncs; worst case loses ~1s of writes.
- **Group commit:** mutations from concurrent connections are appended through a single writer
  goroutine (buffered channel); each request blocks until its record is written (and fsynced,
  under `always`). This serializes disk I/O without serializing reads.
- **Snapshots (keep minimal — see plan 05 note above):** a background job (interval + "WAL bytes
  since last snapshot" threshold) writes the full engine state to `snapshot-<LSN>.db`
  (protocol-encoded stream of SET records), then deletes WAL segments entirely below that LSN.
  Snapshotting holds the engine read lock only long enough to iterate (acceptable at this scale;
  copy-on-write is out of scope). Plan 05 replaces this mechanism with compaction, so implement
  the simplest correct version and resist tuning it.
- **Segmented WAL:** roll to a new file every N MB (`wal-<firstLSN>.log`) so truncation is file
  deletion, and plan 04 standbys can be served from segment files.
- **Crash recovery:** on replay, a record with a bad checksum or truncated tail in the *last*
  segment is treated as a partial write — log a warning, truncate there, continue. Corruption in
  earlier segments is a hard startup error.
- **Recovery bypasses the WAL:** replay applies records straight to the engine without re-logging.

## Implementation steps

1. **New package** `internal/wal/`: `Writer` (append, rotate, fsync policies, group commit),
   `Reader` (iterate segments, checksum validation), `Snapshot` (write/load).
2. **Storage layer** (`internal/storage/storage.go`): `Execute` appends to the WAL before calling
   the engine for `SET`/`DEL`; reads bypass the WAL. Inject a `WAL` interface (mockable, and
   no-op when persistence is disabled).
3. **Engine** (`internal/engine/engine.go`): add an iteration method for snapshotting
   (`Range(func(table, key, value string) bool)`) and a bulk-load path for recovery.
4. **Startup** (`cmd/db/main.go`): load snapshot → replay WAL → then start the TCP server.
   Refuse to start on unrecoverable corruption rather than silently serving partial data.
5. **Shutdown:** on context cancellation, stop accepting requests, flush + fsync the WAL, then exit.
6. **Config** (`internal/config/server.go`): new `wal` section — `enabled`, `data_dir`,
   `sync` policy, `segment_size`, `snapshot_interval`. `engine.type: in_memory` stays valid and
   means "WAL disabled" for backward compatibility.

## Testing

- Unit: record round-trip, checksum rejection, segment rotation, truncated-tail recovery.
- Crash-consistency test: write N keys, `kill -9` the process (test harness), restart, verify
  all acked writes present (under `sync: always`) / all-but-last-second (under `everysec`).
- Recovery equivalence: snapshot+replay state == pre-restart state (property-style test with
  random command sequences).
- Benchmark: throughput under each fsync policy, to document the tradeoff.

## Out of scope

- Copy-on-write snapshots (fork-style), incremental snapshots.
- Any change to the client protocol — persistence is invisible to clients.
