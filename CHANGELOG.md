# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses [Semantic Versioning](https://semver.org/).
Before v1.0.0, a minor release may break compatibility; entries marked **Breaking** say what changed. From v1.0.0 on,
[COMPATIBILITY.md](COMPATIBILITY.md) defines what a v1.x release may not break.

Feature PRs collect changes under the planned next version; its GitHub release uses the matching `vX.Y.Z` tag.
Prerelease tags such as `vX.Y.Z-rc.1` can share that version's section.
The actual release date is recorded when the next version is started.

## [1.0.0] - Unreleased

### Added

- Release verification against checksum-verified archives: CLI contracts, lost/partial reply mutation safety, abrupt
  recovery, corruption preservation, lifecycle checks, frozen v1 fixtures and the offline restore drill.
- A reproducible Linux soak with state verification, WAL rotation, snapshots, restart/restore, connection limits and
  resource ceilings and sustained-growth checks. CI runs short checks on PRs and offers a manual three-hour soak
  for v1 RC sign-off. Both supported Linux archives and exact image digests are verified on native runners before
  release archives and image tags are published.

### Removed

- `RELEASING.md` is no longer part of the repository or the release archives. The supported-platform table moved to
  the README's [Platforms](README.md#platforms) section.

### Fixed

- A pooled call that hit its own deadline could mark a healthy server failed, sending later reads to a standby for the
  whole failure timeout. Expired deadlines are now always treated as the caller giving up.

## [0.14.0] - 2026-10-04

### Added

- `LICENSE` (MIT), this changelog, and `RELEASING.md`, the release checklist.
- Release artifacts: publishing a GitHub release for `vX.Y.Z` builds server and CLI archives for Linux (amd64, arm64),
  macOS (amd64, arm64) and Windows (amd64) with a `SHA256SUMS` file, and a multi-arch container image at
  `ghcr.io/outofstack/db`. `make dist` builds the same archives locally.
- Declared supported platforms: durable deployments are supported on Linux amd64 and arm64 only.
- CI runs on pull requests, pins the golangci-lint version, and smoke-tests the container image (start, query,
  SIGTERM, restart, query).

### Fixed

- A panicking command no longer takes the server down. Its connection is closed without a reply, so the client reports
  the outcome as unknown; the stack trace is logged; and the storage is fenced — data commands answer `UNAVAILABLE`,
  `PING` and `STATUS` keep answering, and a restart recovers from disk. The README had claimed panic recovery before it
  existed.

## [0.13.0] - 2026-09-27

### Added

- `PING` (liveness) and `STATUS` (readiness, durability watermarks, versions) commands.
- A `MANIFEST` in every durable data directory, validated against the configuration at startup.
- CLI quiet mode (`-q`) and exit codes for scripts: `0` all commands succeeded, `1` any failed, `2` invalid usage.
- [docs/operations.md](docs/operations.md), covering backup, restore, crash guarantees and capacity, and an offline
  backup and restore drill that CI runs on every push.

### Changed

- `TABLES` and `KEYS` refuse, with `TOOLARGE`, a listing that would exceed `network.max_message_size`, and stop
  collecting as soon as it does.

### Fixed

- The tiered engine flushes on shutdown, and WAL pruning tracks its fsyncs.

## [0.12.0] - 2026-09-26

### Added

- [COMPATIBILITY.md](COMPATIBILITY.md): the v1 Go API, wire subset, error codes, configuration and storage formats.
- Frozen golden fixtures under `internal/compat` that fail the build when a format would be read or written
  differently.
- `-version` on both binaries: release, commit, protocol and storage format versions.
- `client.ServerError.Code` and `client.ErrWrongType`.

### Changed

- **Breaking:** error replies carry specific stable codes (`PROTOCOL`, `UNKNOWNCMD`, `ARITY`, `ARGUMENT`, `TOOLARGE`,
  `WRONGTYPE`, `READONLY`, `UNAVAILABLE`) instead of a uniform `ERR`.
- **Breaking:** `client.Raw` returns an error reply as a `*ServerError` instead of as response text with a nil error.

## [0.11.0] - 2026-09-19

### Changed

- **Breaking:** non-loopback client and replication listeners require `network.allow_remote` or
  `DB_ALLOW_REMOTE=true`.
- Replication connections and handshakes are bounded by configurable limits and timeouts.
- Info-level logs no longer contain keys, values or error details; those are debug-only.

### Fixed

- The tiered engine latches its first fsync failure, verifies checksums on cold reads, and refuses a record length that
  points past the end of a segment instead of truncating acknowledged data.
- A standby verifies resync snapshot sizes against configurable limits, and a failed resync or a diverged history
  fences it until restart.

## [0.10.0] - 2026-09-08

### Fixed

- **Breaking:** snapshots use format version 3, verified end to end before any record is applied or any WAL segment
  pruned. Older snapshots are refused, with no automatic migration.
- A WAL checksum mismatch aborts startup with the segment and offset instead of being discarded as a torn tail.

## [0.9.0] - 2026-08-20

### Changed

- Configuration paths, fields, log levels, timeouts and sizes are validated strictly; a missing `-config` file aborts
  startup instead of falling back to defaults.
- Durable data directories are locked before recovery, and the ephemeral mode refuses to start over recognized data
  files unless `-allow-ephemeral-over-data` is passed.
- Shutdown drains connections before closing persistence in order, and reports close failures.

## [0.8.0] - 2026-08-14

### Changed

- **Breaking:** client commands take a context that bounds the whole call and can cancel it in flight.
- **Breaking:** a mutation is never re-sent once its frame may have reached a server; an uncertain mutation returns
  `client.ErrOutcomeUnknown`.
- Client connections are lazy, self-healing, and final after `Close`.

## [0.7.0] - 2026-08-08

### Changed

- **Breaking:** the support boundary is explicit: the `tiered` engine, replication and the ephemeral mode are preview
  and log a startup warning.
- **Breaking:** a connection pool accepts exactly one master and routes writes only to it; writes reroute only after a
  manual `PROMOTE`.
- `PROMOTE` is refused unless `replication.allow_remote_promote` is set.

## [0.6.0] - 2026-08-04

### Added

- Typed values (string, int, float, bool, array, map) and the atomic `INCR`, `APPEND`, `HSET`, `HGET` and `TYPE`
  commands.

### Changed

- **Breaking:** a value's type comes from its literal syntax, and `GET` renders it back in that syntax.

## [0.5.0] - 2026-08-01

### Added

- The `tiered` engine: values in on-disk segments behind an LRU cache, with background compaction.

## [0.4.0] - 2026-07-27

### Added

- Optional write-ahead log with `always`, `everysec` and `no` fsync policies, periodic snapshots, and crash recovery.
- Asynchronous master/standby replication over the WAL, `PROMOTE`, and `REPLICATION STATUS`.

## [0.3.0] - 2026-07-14

### Added

- `TABLES`, `EXISTS` and `KEYS` introspection commands.

## [0.2.0] - 2026-07-12

### Changed

- **Breaking:** the wire protocol is RESP2. The CLI keeps its human-readable syntax and encodes it.

## [0.1.0] - 2026-07-12

The v0.1.1 and v0.1.2 tags mark the same changes.

### Added

- The public Go client package, `github.com/OutOfStack/db/client`; the CLI is built on it.
- A Dockerfile.

### Changed

- **Breaking:** keys are scoped by table: `SET <table> <key> <value>`.

## [0.0.1] - 2026-07-12

### Added

- TCP server and CLI client with `SET`, `GET` and `DEL`, YAML configuration, and structured logging.
- Client-side master/standby connection pool with selection strategies, retries and read failover.

[1.0.0]: https://github.com/OutOfStack/db/compare/v0.14.0...HEAD
[0.14.0]: https://github.com/OutOfStack/db/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/OutOfStack/db/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/OutOfStack/db/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/OutOfStack/db/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/OutOfStack/db/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/OutOfStack/db/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/OutOfStack/db/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/OutOfStack/db/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/OutOfStack/db/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/OutOfStack/db/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/OutOfStack/db/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/OutOfStack/db/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/OutOfStack/db/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/OutOfStack/db/compare/v0.0.1...v0.1.0
[0.0.1]: https://github.com/OutOfStack/db/releases/tag/v0.0.1
