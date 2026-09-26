# Operations

What an operator needs to run a v1 server: health checks, what each sync policy promises after a crash, backup and
restore, what to do about corruption, upgrade and rollback rules, and the limits the server enforces. It covers the
supported configuration — the `in_memory` engine with the WAL enabled. Notes on the preview `tiered` engine and
replication are marked as such.

## Health checks

Both commands work on a direct connection with the CLI or any RESP2 client, including while the storage is degraded or
fenced into a terminal state.

**`PING`** replies `PONG`. It proves the process accepts connections and runs commands — a liveness check. It says
nothing about whether writes can be persisted.

**`STATUS`** is the readiness check: one reply of fixed shape, alternating field names and values, with no per-key data
and no configuration secrets. Its size does not grow with the dataset, so it is safe to poll.

```
$ echo STATUS | ./bin/db-cli
ready
true
state
ok
...
```

| Field | Meaning |
|-------|---------|
| `ready` | `true` while the node serves and persists commands. `false` once storage is terminal, and while shutting down |
| `state` | `ok`; `degraded` — snapshot or compaction maintenance is failing, commands are still served and persisted; `terminal` — a restart is required |
| `error` | the terminal or maintenance cause (at most 256 bytes); empty when `state` is `ok` |
| `role` | `standalone`, or with replication (preview) `master` / `standby`. A promoted standby reports `master` |
| `engine` | `in_memory` or `tiered` |
| `durability` | the sync policy — `always`, `everysec`, `no` — or `ephemeral` for the in-memory engine without a WAL |
| `applied_lsn` | the last LSN written to this node's WAL. `0` without one |
| `synced_lsn` | the sync watermark: every record up to this LSN is known to be fsynced (see [sync policies](#sync-policies-and-crash-recovery)) |
| `snapshot_lsn` | the LSN of the newest snapshot on disk, including the one recovery loaded. `0` when there is none |
| `last_sync` | RFC 3339 UTC time of the last successful fsync by this process; empty before the first. Under `no` only a snapshot sets it |
| `last_snapshot` | RFC 3339 UTC time of the last snapshot this process wrote; empty before the first |
| `release`, `protocol` | the build's release and wire protocol, as `-version` prints them |
| `wal_format`, `snapshot_format`, `segment_format` | the on-disk format versions this build reads and writes |

A readiness probe should require `ready` to be `true`. `state degraded` deserves an alert but not traffic removal: the
WAL is still authoritative and every acknowledged write is still persisted under its policy, but the WAL keeps growing
until snapshots succeed again. The field set is part of the wire contract ([COMPATIBILITY.md](../COMPATIBILITY.md)); a
later v1.x release may append fields, so parse by name rather than by position.

`STATUS` describes one node, so a connection pool refuses it — connect to the server you want to ask. `PING` through a
pool is answered by whichever server the pool selects.

## Sync policies and crash recovery

`wal.sync` decides when a write is acknowledged relative to its fsync. A clean shutdown (SIGTERM or SIGINT, followed by
exit status 0) fsyncs everything under every policy; the policies differ only in what survives a crash — the process
being killed, the host losing power, or the kernel panicking.

| Policy | Acknowledged when | What recovery guarantees after a crash |
|--------|-------------------|----------------------------------------|
| `always` | the record is fsynced (concurrent writes share one fsync) | every acknowledged write |
| `everysec` | the record is written; a background fsync runs every second | every write up to the sync watermark at the time of the crash |
| `no` | the record is written; the WAL is fsynced only at shutdown and when a snapshot is written | every write up to the sync watermark, which only moves at those two points |

Under `everysec` and `no`, writes acknowledged after the watermark usually survive a process crash — they reached the
operating system — but the guarantee is only the watermark: they are lost if the host goes down with them. `synced_lsn`
in `STATUS` is that watermark. Recovery always rebuilds a contiguous prefix of the log: it never applies a later record
after skipping an earlier one.

A failed fsync is never retried as if nothing happened: the WAL latches it as terminal, every later write is refused
with `UNAVAILABLE`, `STATUS` reports `state terminal`, and the process exits nonzero on shutdown. Restart it once the
underlying storage problem is fixed; recovery replays what reached the disk.

The preview `tiered` engine offers the same three policies on `engine.sync` with the same meanings — including the
fsync of every segment at a clean shutdown — and latches a failed fsync the same way. It writes no snapshots, so under
`no` it fsyncs only at shutdown.

## Backup

Backup is offline: stop the server cleanly and copy its data directory. There is no online or incremental backup.

1. Stop the server with SIGTERM (or SIGINT) and wait for it to exit. **The exit status must be 0.** A nonzero exit
   means shutdown could not flush or close the storage — the log names the cause — and the directory is not a clean
   backup until that is resolved.
2. Copy the **complete** data directory (`wal.data_dir`, or `engine.data_dir` for the tiered engine), preserving file
   names: `cp -a data/ backup/`. Do not pick files: recovery needs the newest snapshot *and* every WAL segment after it,
   and the `MANIFEST` is what lets a restore be checked.
3. Restart the server.

The directory holds:

| File | Purpose |
|------|---------|
| `MANIFEST` | the engine, the format versions of its files, and the sync policy they were written under (JSON). Written at every startup, after recovery accepted the files |
| `snapshot-<lsn>.db` | snapshots (in-memory engine) |
| `wal-<lsn>.log` | WAL segments (in-memory engine) |
| `seg-<n>.data` | segments (tiered engine) |
| `.unverified-history` | present only on a replication master that recovered from a torn tail (preview); keep it with the backup |
| `.db.lock` | the process lock. Copying it is harmless; it carries no data |

A backup is as good as the last restore drill that used it. [`scripts/restore-drill.sh`](../scripts/restore-drill.sh)
(`make restore-drill`) runs this whole procedure against the real binaries — populate, stop, copy, wipe, restore,
compare every value — and runs in CI on every push.

## Restore

1. Create a new, **empty** directory and copy the backup's files into it: `mkdir data && cp -a backup/. data/`.
2. Point the configuration at it with the **same engine** the backup was taken from, and start the server.
3. Check `STATUS` (`ready true`, `state ok`, `snapshot_lsn` as expected) and read back a few known keys.

Startup checks the restored directory before recovery opens any of it, and refuses — without modifying a file — when:

- the `MANIFEST` names another engine than the configuration selects, or its format versions differ from what this
  build writes (compare with `db -version`);
- the files are another engine's, or a mix of both engines';
- another process holds the directory's lock.

A directory without a `MANIFEST` (copied from a server that never started with this release) is recovered from its
files, which carry their own format headers, and gains a `MANIFEST` on that first start.

## Corruption

Recovery repairs exactly one thing on its own: a record cut short at the very end of the newest WAL segment (or tiered
segment), which is what a crash leaves. Everything else fails startup with an error naming the file (and the byte offset,
for a damaged record) and leaves the file unchanged:

- a checksum mismatch anywhere, including in the final record;
- a snapshot that fails its checksum, when no older verified snapshot plus the retained WAL covers its state;
- a file whose format header carries an unknown version.

The response is to **restore from backup**. Do not edit, truncate or delete files to get past the error: a checksum
mismatch cannot be told apart from a flipped acknowledged record, which is why the server refuses to guess, and removing
a segment leaves a gap in the log that recovery refuses as well. There is no repair mode. Without a backup, keep a copy
of the damaged directory for investigation and start over on a fresh one.

A snapshot or prune failure while running is not corruption: it is reported as `state degraded`, retried on the next
snapshot interval, and the WAL keeps every record meanwhile.

## Upgrades, rollback and pre-v1 data

- **Take a backup before every upgrade.** It is the only rollback that works in every case.
- **Within v1.x**, a release reads every directory an earlier v1.x release wrote
  ([COMPATIBILITY.md](../COMPATIBILITY.md)). Upgrading is: clean stop, back up, start the new binary on the same
  directory.
- **Rolling back** to an earlier v1.x binary works on the same directory when that binary's `-version` storage line
  matches the `formats` in `MANIFEST`. If they differ, the older binary refuses the directory at startup without
  changing it; restore the pre-upgrade backup instead, accepting that writes made since the upgrade are lost.
- **Across major versions** there is no in-place rollback: restore the backup taken before the upgrade.
- **Pre-v1 data has no upgrade path.** Start a v1 server on a fresh data directory or on a backup taken from a v1
  server. Files carrying an older format version are refused at open; there is no export or import tooling. Directories
  written by v0.10.x and v0.11.x happen to use the v1 format versions and will open, but they are outside the v1
  compatibility promise.

## Limits

| Limit | Default | Behavior when exceeded |
|-------|---------|------------------------|
| `network.max_message_size` (KiB) on a request | 4 | the request is refused with `TOOLARGE` |
| `network.max_message_size` on a `TABLES` or `KEYS` listing | 4 | the listing is refused with `TOOLARGE` — never truncated. The server stops collecting names once the reply would pass the limit, so an oversized listing costs at most the limit in memory. Raise the limit, and the client's `max_message_size` with it, to list larger tables |
| other replies | — | not bounded by the server: an array or map grown past the limit one `APPEND` or `HSET` at a time is returned in full, and a client whose own limit is smaller fails locally |
| `network.max_connections` | 100 | further connections are closed on accept |
| table name | 128 bytes | `TOOLARGE` |
| one WAL record | 64 MiB | `TOOLARGE`; only reachable with `max_message_size` above 64 MiB |

Memory: the in-memory engine holds the entire dataset in RAM. A snapshot briefly pauses writes while it copies the
list of entries (not the values themselves) and writes the file afterwards, so plan for one extra reference per key
while a snapshot runs and for disk space of about two snapshots plus the WAL since the older one.

The preview tiered engine keeps every key, but only `engine.max_memory` MiB of values, in RAM; live data past
`engine.max_storage` MiB is refused with `TOOLARGE storage full`.
