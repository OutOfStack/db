# Plan 04 — Replication (Master Writes, Standby Reads)

## Description

Turn the client pool's master/standby labels into a real topology: one master accepts writes and
streams its WAL to standbys, which apply it in order and serve reads. Replication is asynchronous
(the master acks the client without waiting for standbys). Failover is manual via a `PROMOTE`
admin command — no automatic election.

## Relationship to other plans

- **Depends on plan 03 (WAL persistence):** the WAL *is* the replication stream; standbys sync by
  requesting records from a given LSN.
- **Depends on plan 02 (Protocol framing):** server-to-server messages use the reserved
  version/type tag byte to add replication message types.
- **Independent of plan 01 (Tables)** beyond what plan 03 already absorbed — replication ships
  opaque WAL records and never inspects keys.

## Design decisions

- **Roles via config:** `replication.role: master | standby`, standbys get
  `replication.master_address`. A standby rejects `SET`/`DEL` with a distinguishable error
  (`ERR readonly`) so the pool client can react.
- **Log shipping:** a standby connects to the master and sends `REPLICATE <lsn>`; the master
  streams every WAL record with LSN > lsn (reading historical segments from disk, then live
  records from an in-memory fan-out in the WAL writer). Standbys persist received records to
  their *own* WAL before applying — a promoted standby has a complete log to serve from.
- **Resync:** if the requested LSN is older than the master's oldest retained segment
  (truncated by snapshotting), the master sends its latest snapshot first, then streams from the
  snapshot LSN. This makes bootstrap of an empty standby and catch-up after long downtime the
  same code path.
- **Asynchronous only.** Documented consequence: reads from standbys can be stale; a client
  needing read-your-writes uses the `master_first` pool strategy.
- **Manual promotion:** `PROMOTE` flips a standby to master (stops the replication client, starts
  accepting writes). Demoting/repointing the old master and re-aiming other standbys is an
  operator action in this iteration. No quorum, no split-brain detection — explicitly documented.
- **Pool routing by command type** (`internal/pool/client.go`): `Send` gains a read/write hint
  derived from the command. Writes always route to masters (standbys are skipped, and an
  `ERR readonly` response marks the routing stale and triggers re-selection); reads follow the
  configured strategy. `ServerConfig` already distinguishes master/standby, so
  `internal/pool/selector.go` needs a `SelectWrite()` / `SelectRead()` split of the current
  `Select()`.

## Implementation steps

1. **WAL fan-out** (`internal/wal/`): the writer goroutine from plan 03 additionally publishes
   committed records to registered subscribers (buffered; a slow standby falls back to reading
   segments from disk rather than blocking the master).
2. **New package** `internal/replication/`:
   - `Master`: handles `REPLICATE` connections — snapshot send, segment catch-up, live tail.
   - `Standby`: connect/retry loop, persist + apply records, track applied LSN, expose lag.
3. **Server wiring** (`cmd/db/main.go`): start `Master` or `Standby` per config; standby mode
   installs a write-rejecting wrapper around the compute handler.
4. **Admin commands:** `PROMOTE` (and `REPLICATION STATUS` returning role, applied LSN, lag) —
   added to `internal/parser` and handled above the storage layer, not written to the WAL.
5. **Pool client** (`internal/pool/`): read/write routing, `ERR readonly` handling, selector
   split, config already has master/standby lists.
6. **Config** (`internal/config/server.go`): `replication` section — `role`, `master_address`,
   reconnect backoff.

## Testing

- Integration (in-process, two servers on ephemeral ports): write to master → read from standby
  after lag settles; standby restart resumes from its LSN; standby down long enough to need
  snapshot resync; `PROMOTE` then writes succeed on new master.
- Pool: writes never hit standbys; `ERR readonly` triggers failover to master.
- Race detection across the whole suite (`make test`), given WAL fan-out adds cross-goroutine
  paths.

## Out of scope

- Automatic failover / leader election (Raft) — manual `PROMOTE` only.
- Synchronous or quorum replication.
- Cascading replication (standby of a standby).
