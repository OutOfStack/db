# Simple! Database Server

A networked key-value database with TCP server and CLI client written in Go. Replication is available as a preview
feature (see [Support Boundary](#support-boundary)).

## Architecture

The project consists of three main components:
- **Database Server** (`cmd/db`): TCP server that handles database operations
- **CLI Client** (`cmd/db-cli`): Command-line client for interacting with the server
- **Go Client Library** (`client`): Public package for using the database from Go programs

## Features

- TCP-based client-server architecture
- Concurrent client handling with connection limiting
- YAML configuration support with command-line overrides
- Structured logging with configurable levels
- Graceful shutdown with proper resource cleanup
- Command-line interface for database operations
- Two storage engines: `in_memory` (RAM-only) and `tiered` (preview), whose dataset grows past RAM by keeping values
  in on-disk segments behind an LRU cache
- Tables: keys are scoped per table, created implicitly on first write
- Typed values (string, int, float, bool, array, map) with server-side atomic operations: `INCR`, `APPEND`,
  `HSET`/`HGET`, `TYPE`
- Durability: write-ahead log with `always`/`everysec`/`no` fsync policies, periodic snapshots, and crash recovery that
  truncates a torn tail
- Replication (preview): asynchronous master/standby WAL shipping with manual `PROMOTE`
- Operations: `PING` and `STATUS` health checks, self-describing data directories, and a documented offline backup and
  restore procedure that CI drills on every change (see [docs/operations.md](docs/operations.md))
- Connection limiting to prevent resource exhaustion
- **Master/Standby Connection Pooling** with read failover and retry; writes reroute only after a manual promotion
- Configurable server selection strategies (master_first, round_robin, random)

## Support Boundary

Generally available, supported for production use:

- The `in_memory` engine with WAL persistence and snapshots
- The public Go client library and the CLI
- The documented RESP2 command subset and typed literals
- Deployment on loopback or an explicitly trusted private network
- Linux on amd64 or arm64. macOS and Windows builds are provided for development and evaluation, but durable
  deployments there are not supported (see [Platforms](#platforms))

What a v1.x release promises about each of these — the Go API, the wire subset and error codes, the configuration, and
the on-disk formats — is written down in [COMPATIBILITY.md](COMPATIBILITY.md).

Preview — limited support, marked by a startup warning:

- The `tiered` engine
- Replication: master/standby WAL shipping, standby reads, and `PROMOTE`
- Ephemeral mode (the in-memory engine with `wal.enabled: false`): data lives only in RAM and is lost on shutdown.
  Note this is the default configuration — durability is opted into by setting `wal.enabled: true`

Preview caveats:

- Standby reads may be stale: replication is asynchronous, with no read-your-writes guarantee
- There is no automatic write failover. If the master fails, writes fail until an operator isolates the old master,
  promotes a standby with `PROMOTE` (requires `replication.allow_remote_promote`), and points clients at the new master
- A pool routes writes to its single configured master; configs listing more than one master are rejected
- Preview failures are contained, not repaired. A standby whose resync fails part-way, or whose history diverges from
  the master's, enters a terminal state: it refuses every command, stops replicating, refuses `PROMOTE`, and
  `REPLICATION STATUS` reports `state terminal` with the cause. Restart it, after emptying its data directory when the
  message says to reseed. A master that recovered from a torn WAL tail under `everysec` or `no` records that in its
  data directory and refuses, across restarts, to resume any standby from a retained LSN, because that standby may
  hold the record the master lost. Reseed every standby, then restart the master once with
  `-clear-unverified-history`. Divergence is otherwise detected only by LSN comparison, so a master that rolls back
  and then advances past a standby's LSN before it reconnects is not detected — the replication GA timeline protocol
  closes that gap
- The tiered engine latches its first fsync failure: later writes are refused with `UNAVAILABLE tiered engine is in a
  terminal state`, reads continue, and the process exits nonzero on shutdown. A checksum mismatch in a segment is
  corruption — a cold read of that key fails while other keys stay readable, and a restart refuses to start until the
  file is restored. Only a record header cut short at the end of the newest segment is truncated as a crash tail; a
  header whose lengths reach past the end of the file is reported with its offset and left in place, since a crash
  and a corrupt length look the same there and truncating would delete every intact record behind it

### Platforms

| Platform | Status |
|----------|--------|
| Linux amd64, arm64 | Supported, including durable (WAL or tiered) deployments. The container image is Linux only |
| macOS amd64, arm64 | Development and evaluation only; durable deployments unsupported |
| Windows amd64 | Development and evaluation only; durable deployments unsupported |

Crash consistency is untested on macOS and Windows. The server also does not fsync directories on Windows.

## Commands

Commands are entered in the CLI using the simple syntax below and sent to the server as RESP2 frames (see [Network
Protocol](#network-protocol)). Every key belongs to a table: tables are created implicitly on the first `SET` and
removed automatically when their last key is deleted.

### Value types

Values are typed. The type comes from the literal syntax of the value, and `GET` returns a human-readable
representation:

| Literal | Type | Notes |
|---------|------|-------|
| `hello world` | string | anything that is not one of the forms below |
| `"42"` | string | quoting forces text that would otherwise be a number |
| `42`, `-7` | int | 64-bit; `INCR` past the range is an error, never a wraparound |
| `42.5`, `1e3` | float | a whole float renders back as `1.0`, so its type survives |
| `true`, `false` | bool | |
| `[1,"two",true]` | array | JSON syntax; elements may be any type, including nested |
| `{"a":1,"b":[2]}` | map | JSON syntax; keys are strings, values any type |

A string renders bare, so a plain `SET`/`GET` round-trips to exactly the text that was stored; strings nested in an
array or map render quoted. Use `TYPE` to see what a key actually holds.

**Quoting in the CLI.** The CLI splits an input line the way a shell does: it uses `'` and `"` to group a token and
removes them. A literal that contains double quotes or spaces therefore has to be wrapped in single quotes, or the
server never sees it as written:

```
SET t conf '{"a":1}'      # map     — without the single quotes: {a:1}, an error
SET t tags '["a","b"]'    # array   — without them: [a,b], an error
SET t zip '"01234"'       # string  — without them: 01234, an int
SET t nums [1,2,3]        # no quotes or spaces inside, so it needs no wrapping
SET t path 'C:\tmp'       # single quotes are literal: \n, \t and \ are not escapes inside them
```

Programs using the client library pass the literal directly and need none of this — the quoting is a property of the
CLI's line splitting, not of the syntax.

### SET
Set a key-value pair in a table:
```
SET <table> <key> <value>
```
Example:
```
SET users name John
SET users age 42
SET users tags [1,2,3]
```

### GET
Get value by key from a table:
```
GET <table> <key>
```
Example:
```
GET users name
```

### DEL
Delete key from a table:
```
DEL <table> <key>
```
Example:
```
DEL users name
```

### TABLES
List all tables in sorted order:
```
TABLES
```

### EXISTS
Report whether a table currently contains any keys:
```
EXISTS <table>
```

### KEYS
List all keys in a table in sorted order. A missing table returns an empty list.
```
KEYS <table>
```
A `TABLES` or `KEYS` listing whose reply would exceed the server's `network.max_message_size` is refused with
`TOOLARGE` rather than truncated, and the server stops collecting names as soon as it passes the limit. There is no
pagination; raise the limit (on the server and the client) to list larger tables.

### TYPE
Report the type of a stored value (`string`, `int`, `float`, `bool`, `array`, `map`):
```
TYPE <table> <key>
```

### INCR
Add `delta` (default `1`) to a numeric value and return the new value. A missing key starts at `0`. Int arithmetic stays
exact; a float operand makes the result a float. Concurrent increments are atomic.
```
INCR <table> <key> [delta]
```
Example:
```
INCR stats hits
INCR stats hits 10
INCR stats ratio 0.5
```

### APPEND
Push a value onto an array and return the new length. A missing key becomes a new array:
```
APPEND <table> <key> <value>
```

### HSET / HGET
Set and read one field of a map value. A missing key becomes a new map; reading a missing field replies like a missing
key:
```
HSET <table> <key> <field> <value>
HGET <table> <key> <field>
```
Example:
```
HSET users u1 name John
HSET users u1 age 42
HGET users u1 age
```

Running a typed command against a value of another type is an error and changes nothing:
```
WRONGTYPE wrong type: key holds array, INCR requires int or float
```

### PING
Liveness check: replies `PONG` whenever the server is accepting commands, including when its storage is fenced.
```
PING
```

### STATUS
Readiness check for one server: a fixed list of field/value pairs — `ready`, `state` (`ok`, `degraded`, `terminal`),
`error`, `role`, `engine`, `durability`, the applied and synced LSNs, the newest snapshot's LSN, the times of the last
fsync and snapshot, and the release and format versions. It carries no per-key data and its size does not depend on
the dataset, so it is safe to poll. [docs/operations.md](docs/operations.md#health-checks) describes every field. A
connection pool refuses `STATUS`, because it cannot say which server would answer; connect to the server directly.
```
STATUS
```

## Configuration

### Server Configuration

The server can be configured using a YAML file. Example configuration:

```yaml
engine:
  type: "in_memory"
wal:
  enabled: true
  data_dir: "data"
  sync: "everysec"
  segment_size: 64
  snapshot_interval: 5m
network:
  address: "127.0.0.1:3223"
  max_connections: 100
  max_message_size: 4
  idle_timeout: 5m
  shutdown_timeout: 10s
logging:
  level: "info"
  output: "/log/output.log"
```

#### Server Configuration Options

- **engine.type**: `in_memory` (RAM-only) or `tiered` (memory over disk)

The `engine.*` options below apply only to the tiered engine. It carries its own durable store, so it cannot be combined
with `wal.enabled` or replication — the server refuses that combination at startup.

- **engine.data_dir**: Directory for the tiered segment store
- **engine.max_memory**: MiB of hot values kept in RAM (the LRU budget)
- **engine.max_storage**: MiB ceiling on live data; `SET` past it returns `TOOLARGE storage full`
- **engine.sync**: Fsync policy for segments (`always`, `everysec`, or `no`)
- **engine.segment_size**: Segment file size in MiB
- **engine.compaction_threshold**: Reclaim a sealed segment once this fraction of it is dead bytes
- **engine.compaction_interval**: How often to compact and log cache/disk stats

- **wal.enabled**: Enable durable write-ahead logging (disabled by default)
- **wal.data_dir**: Directory for WAL segments and snapshots. Required for the in-memory engine even when WAL is
  disabled, because startup scans it before allowing ephemeral mode
- **wal.sync**: Fsync policy (`always`, `everysec`, or `no`)
- **wal.segment_size**: WAL segment rollover size in MiB
- **wal.snapshot_interval**: Interval between snapshots when data has changed
- **replication.role**: `""` (standalone), `master`, or `standby`; requires `wal.enabled`
- **replication.listen_address**: Master: where standbys connect for the WAL stream. Standby: optional, so `PROMOTE` can
  start serving replication from this node
- **replication.master_address**: Standby: the master to replicate from
- **replication.reconnect_backoff**: Standby: pause between reconnect attempts
- **replication.allow_remote_promote**: Standby: permit the `PROMOTE` command over the client port (default `false`)
- **replication.max_connections**: Maximum concurrent master connections, including handshakes (default `100`)
- **replication.handshake_timeout**: Whole master handshake deadline; standby dial and handshake-write timeout
  (default `10s`)
- **replication.idle_timeout**: Timeout for each streaming socket read/write, including snapshot transfers (default `1m`).
  Standby configuration requires a value greater than `1s`, the maximum master heartbeat interval; silent masters
  trigger a reconnect. Leave margin for scheduling and network delays (the default is recommended)
- **replication.max_snapshot_size**: Standby: largest resync snapshot accepted, in MiB, checked against the declared
  frame length before any of it is read (default `4096`)
- **replication.max_snapshot_entries**: Standby: most entries a resync snapshot may hold; the load stops at the cap
  before the next entry is kept in memory (default `10000000`)
- **network.address**: Server listening address (default `127.0.0.1:3223`)
- **network.allow_remote**: Allow non-loopback client and replication listen addresses (default `false`). Wildcard binds,
  private/public IPs, and hostnames (including `localhost`) require this opt-in; use literal `127.0.0.1` or `::1` locally
- **network.max_connections**: Maximum concurrent client connections (enforced by server)
- **network.max_message_size**: Maximum message size in KB
- **network.idle_timeout**: Client idle timeout duration
- **network.shutdown_timeout**: Grace period for decoded commands before their contexts are cancelled (default `10s`).
  Shutdown then waits for handlers to return before closing persistence, so this is not a hard process-exit deadline
- **logging.level**: Log level (debug, info, warn, error). Info logs include command name, outcome, duration, and
  table/key byte lengths; arguments and error details are debug-only. Debug can expose values and is unsafe for production
- **logging.output**: Log output file path (empty for stdout)

Unknown YAML fields, unsupported log levels, and byte-size values that overflow are startup errors. Durable modes hold
an OS lock on `.db.lock` in their data directory from before recovery until final close, so a second server using that
directory fails immediately. The lock is advisory on Unix and requires a local filesystem; NFS and other network
filesystems are unsupported. The lockfile remains after shutdown, but the OS lock is released automatically, including
after a process crash.

#### Environment Variable Overrides

Server settings can be overridden with environment variables, which take the highest priority (environment > config file
> defaults):

- `DB_ADDRESS` — listening address
- `DB_ALLOW_REMOTE` — explicit trusted-network opt-in (`true`/`false`), covering both client and replication listeners
- `DB_MAX_CONNECTIONS` — maximum concurrent client connections
- `DB_MAX_MESSAGE_SIZE` — maximum message size in KB
- `DB_IDLE_TIMEOUT` — client idle timeout (Go duration, e.g. `5m`)
- `DB_LOG_LEVEL` — log level
- `DB_LOG_OUTPUT` — log output file path

## Installing

Each release on the [releases page](https://github.com/OutOfStack/db/releases) carries an archive per platform
(`db_<version>_<os>_<arch>.tar.gz`, holding both `db` and `db-cli`) and a `SHA256SUMS` file. Check the archive before
unpacking it:

```bash
VERSION=v1.0.0  # the release to install
curl -LO "https://github.com/OutOfStack/db/releases/download/$VERSION/db_${VERSION}_linux_amd64.tar.gz"
curl -LO "https://github.com/OutOfStack/db/releases/download/$VERSION/SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS
tar -xzf "db_${VERSION}_linux_amd64.tar.gz"
"./db_${VERSION}_linux_amd64/db" -version
```

The server image is published for `linux/amd64` and `linux/arm64` as `ghcr.io/outofstack/db:<version>` (without the
leading `v`, e.g. `1.0.0`); see [With Docker](#with-docker) for how to run it. To build from source instead, see
[Building](#building).

## Running the Server

The server has no authentication or TLS. Use loopback or an explicitly trusted, isolated private network; public or
untrusted network exposure is unsupported. `network.allow_remote` / `DB_ALLOW_REMOTE` acknowledges this boundary and
provides no access control. Anyone who can reach either listener can access data; enabling
`replication.allow_remote_promote` also permits any client to promote a preview standby. See [SECURITY.md](SECURITY.md)
for deployment and vulnerability-reporting guidance.

### With default configuration:
```bash
make build
./bin/db
```

### With custom configuration:
```bash
./bin/db -config config.yaml
```

A non-empty `-config` path is exact and required: relative and absolute paths are accepted, while a missing or unreadable
file aborts startup instead of falling back to defaults. Starting the ephemeral in-memory mode over recognized WAL,
snapshot, or tiered files is also refused. To acknowledge that data will be ignored for one launch, pass
`-allow-ephemeral-over-data`; this flag never permits opening one durable engine's files with the other engine.
`-clear-unverified-history` removes the marker a replication master leaves after recovering from a torn WAL tail (see
the preview caveats); pass it only after every standby has been reseeded.

WAL recovery truncates only an incomplete record at EOF in the final segment. A checksum mismatch, including in the
last record, aborts startup with the segment path and byte offset and leaves the file unchanged. Restore corrupted
recovery files from a backup; startup never discards checksum-invalid records automatically.

Snapshots use format version 3: the header, snapshot LSN, and all records are covered by a trailing CRC32 checksum and
completion marker. Verification finishes before applying any snapshot records or pruning WAL segments. Older snapshot
formats are refused; there is no automatic migration. A damaged or incomplete snapshot is skipped only when an older
verified snapshot plus the retained WAL (or the WAL alone) covers its state; otherwise startup fails.

Snapshot or prune failures are logged as degraded maintenance and retried on the next snapshot interval. WAL appends
continue unless an append or WAL sync failure has latched a terminal error. `STATUS` reports these states separately,
along with the latest written LSN and the observed sync watermark. For `everysec` and `no`, unsynced records may survive
a process crash, but their survival is not a durability guarantee.

A durable data directory is self-describing: the server writes a `MANIFEST` naming its engine, format versions and sync
policy, and refuses at startup a directory whose manifest does not match the configuration. Backups are offline — stop
the server cleanly and copy the whole directory. [docs/operations.md](docs/operations.md) covers backup and restore,
what each sync policy guarantees after a crash, corruption, upgrades and rollback, and capacity limits. There is no upgrade path
from pre-v1 data: start v1 on a fresh directory or a backup taken from a v1 server.

### Using make:
```bash
make run
```

### With Docker:
```bash
docker run --rm -p 127.0.0.1:3223:3223 \
  -e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true ghcr.io/outofstack/db:1.0.0
```

The examples below use a locally built image named `db`; `make docker-run` builds and starts it, or build it with
`docker build -t db .`. A published image works the same way in its place.

The image itself defaults to loopback **inside the container**. Publishing a port alone does not make that listener
reachable from the host. The quickstart explicitly opts into binding all container interfaces while publishing only on
host loopback. Other containers on the same Docker network may still reach it: that network must also be trusted.
Logs go to stdout for `docker logs`.

To publish on a trusted private host interface, replace `192.168.1.10` with that host's private IP and restrict access
with your firewall:

```bash
docker run --rm -p 192.168.1.10:3223:3223 \
  -e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true -e DB_MAX_CONNECTIONS=500 db
```

Settings without an environment variable (engine type, WAL, replication) come from a YAML file. A relative `-config`
path is resolved from the working directory, which is `/home/nonroot`, so mount the file there; an absolute mounted path
works as well. A path that does not exist aborts startup:

```bash
docker run --rm -p 127.0.0.1:3223:3223 \
  -e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true \
  -v "$PWD/config.server.yaml:/home/nonroot/db.yaml" \
  -v db-data:/home/nonroot/data \
  db -config db.yaml
```

The image ships an empty `data` directory owned by the `nonroot` user, so a named volume mounted over it inherits that
ownership and the WAL and tiered engine can write to it. Without the volume, data lives in the container's writable
layer and is lost with the container.

## Using the CLI Client

The CLI client supports both configuration files and command-line flags for flexibility.

### Basic Client Configuration

The client can be configured using a YAML file:

```yaml
network:
  address: "127.0.0.1:3223"
  max_message_size: 4
  idle_timeout: 1m
```

### Client with Connection Pool

For a master/standby deployment (exactly one master per pool):

```yaml
network:
  address: "127.0.0.1:3223"
  max_message_size: 4
  idle_timeout: 1m

pool:
  enabled: true

  servers:
    - address: "127.0.0.1:3223"
      role: master
    - address: "127.0.0.1:3224"
      role: standby
    - address: "127.0.0.1:3225"
      role: standby

  selection_strategy: master_first
  max_retries: 3
  retry_delay: 1s
  failure_timeout: 30s
```

#### Pool Configuration Options

- **pool.enabled**: Enable connection pooling (default: false)
- **pool.servers**: List of servers with address and role (master or standby)
- **pool.selection_strategy**: How to select servers from the pool
  - `master_first`: Try master servers first, fall back to standby on failure
  - `round_robin`: Rotate through all servers in order
  - `random`: Pick servers randomly
- **pool.max_retries**: Maximum number of retry attempts when a server fails
- **pool.retry_delay**: Delay between retry attempts
- **pool.failure_timeout**: Time after which failed servers are automatically retried

### Usage Examples

#### Connect with default settings:
```bash
./bin/db-cli
# or
make run-cli
```

#### Connect with configuration file:
```bash
./bin/db-cli --config=client.yaml
```

#### Connect with command-line overrides:
```bash
./bin/db-cli --address=192.168.1.100:3223 --timeout=30s
```

#### Mix configuration file with overrides:
```bash
./bin/db-cli --config=client.yaml --address=localhost:9999
```

### Client Configuration Priority

1. **Command-line flags** (highest priority)
2. **Configuration file values**
3. **Default values** (lowest priority)

### Available CLI Flags

- `--config`: Path to configuration file
- `--address`: Database server address (overrides config)
- `--timeout`: Connection idle timeout (overrides config)
- `-q`: Quiet: print only errors, for scripts that rely on the exit status
- `--version`: Print release, commit, protocol and storage format versions, then exit

### Interactive session example:
```
$ ./bin/db-cli
Connected to database server at localhost:3223
Available commands:
  SET table key value
  GET table key
  DEL table key
  TABLES
  EXISTS table
  KEYS table
  TYPE table key
  INCR table key [delta]
  APPEND table key value
  HSET table key field value
  HGET table key field
  PING
  STATUS
Type 'exit' to quit

> SET users name Alice
OK
> GET users name
Alice
> SET users age 42
OK
> TYPE users age
int
> INCR users age
43
> DEL users name
OK
> exit
```

The CLI also reads commands from stdin, skipping blank and `#` lines, so a prepared script runs end to end. With piped
input it prints no banner or prompt — only the replies on stdout, and errors on stderr prefixed with their line number.
An error reply is reported and the script continues; a line the CLI cannot parse, or a lost connection, stops it. The
exit status is `0` only when every command succeeded, `1` when any failed, and `2` for invalid flags or configuration.

[`examples/smoke.txt`](examples/smoke.txt) runs every standalone command and must exit `0`;
[`examples/errors.txt`](examples/errors.txt) triggers one failure of each kind and exits `1`:

```bash
./bin/db-cli -q < examples/smoke.txt && echo ok
./bin/db-cli < examples/errors.txt; echo "exit status $?"
```

## Go Client Library

External Go programs can use the database through the public client package — the only supported import path for
external consumers:

```go
import "github.com/OutOfStack/db/client"

c, err := client.New(client.WithAddress("127.0.0.1:3223"))
if err != nil {
    return err
}
defer c.Close()

err = c.Set(ctx, "users", "name", "Alice")
val, err := c.Get(ctx, "users", "name") // returns client.ErrNotFound if the key is missing
err = c.Del(ctx, "users", "name")
```

Values are typed by their literal syntax (see [Value types](#value-types)), and the typed operations are available too:

```go
err = c.Set(ctx, "users", "age", "42")       // int
kind, err := c.Type(ctx, "users", "age")     // "int"
hits, err := c.Incr(ctx, "stats", "hits", "")   // "" increments by 1
n, err := c.Append(ctx, "users", "tags", "go")  // new array length
err = c.HSet(ctx, "users", "u1", "name", "Alice")
name, err := c.HGet(ctx, "users", "u1", "name") // ErrNotFound if the field is missing
```

For master/standby deployments, configure a connection pool instead of a single address:

```go
c, err := client.New(
    client.WithServers(
        client.Server{Address: "127.0.0.1:3223", Role: client.RoleMaster},
        client.Server{Address: "127.0.0.1:3224", Role: client.RoleStandby},
    ),
    client.WithStrategy(client.MasterFirst),
    client.WithRetries(3, time.Second),
)
```

Error handling. Three conditions have exported sentinels, because only these are usually acted on rather than reported;
everything else is a `*client.ServerError` whose `Code` is one of the stable [error codes](#error-handling):
- `client.ErrNotFound` — returned by `Get`/`Del` for missing keys (check with `errors.Is`)
- `client.ErrOutcomeUnknown` — the command reached a server but no reply came back, so whether it was applied cannot be
  determined (check with `errors.Is`)
- `client.ErrWrongType` — the key holds another type, or the arithmetic does not fit it (check with `errors.Is`)
- `*client.ServerError` — every other server response; match with `errors.As` and branch on `.Code`, never on the
  message text
- `Raw(ctx, command)` — escape hatch that sends a raw command line and returns the reply as text; an error reply comes
  back as a `*client.ServerError` rather than as text

Delivery semantics: a command that fails is never re-sent once a complete frame may have reached a server, because
repeating `Incr`, `Append` or `HSet` would apply it twice. A command the client could not finish writing is retried — the
server acts on whole frames only, so a truncated one provably did not run. Reads are retried transparently; a mutation
that fails after its frame went out returns
`ErrOutcomeUnknown` instead, and it is the caller's decision whether to re-issue it (safe for an idempotent `Set`) or to
check the current value first. Cancelling a context interrupts a command in flight, and a cancelled mutation can still
have been applied, so that error matches both `ErrOutcomeUnknown` and `context.Canceled`.

`New` validates configuration but does not connect: the first command opens the connection under its own context, so an
unreachable server surfaces there rather than at construction. The client is safe for concurrent use, and `Close` is
idempotent and final — it interrupts commands in flight and later calls fail rather than reconnecting.

Note that `Set` takes a typed literal, parsed server-side, not an unconditionally string-typed value — the int 42 and
the string `01234` are set like this:

```go
err = c.Set(ctx, "users", "age", "42")      // int 42
err = c.Set(ctx, "users", "zip", `"01234"`) // string 01234, quoted so it is not read as a number
```

The full contract these guarantees belong to — the frozen wire subset, error codes, storage formats, and what a v1.x
release promises about each — is in [COMPATIBILITY.md](COMPATIBILITY.md).

## Building

Build both server and client:
```bash
make build
```

Build individual components:
```bash
go build -o bin/db ./cmd/db
go build -o bin/db-cli ./cmd/db-cli
```

## Project Structure

```
├── client/                      # Public Go client library
├── cmd/                         # Command-line applications
│   ├── db/                      # Database server
│   │   └── main.go
│   └── db-cli/                  # CLI client
│       └── main.go
├── examples/                    # CLI scripts: smoke.txt (must exit 0) and errors.txt (fails on purpose)
├── test/release/                # Black-box release checks (Go, `release` build tag) and the soak
├── scripts/
│   ├── container-smoke.sh       # Start, query, SIGTERM and restart the image, run in CI
│   ├── dist.sh                  # Release archives and checksums (make dist)
│   └── restore-drill.sh         # Offline backup and restore drill, run in CI
├── config.client.example.yaml   # Example client configuration
├── config.server.example.yaml   # Example server configuration
├── example-pool-config.yaml     # Example pool configuration
└── internal/                    # Internal packages
    ├── compute/                 # Request handling and command execution
    ├── config/                  # Configuration management
    ├── datadir/                 # Data directory lock, file detection and MANIFEST
    ├── engine/                  # In-memory storage engine
    │   └── tiered/              # Memory/disk engine: segments, keydir, LRU, compaction
    ├── network/                 # TCP networking layer
    ├── parser/                  # Command parsing
    ├── pool/                    # Connection pooling and failover
    ├── protocol/                # RESP2 framing and the typed-value codec
    ├── replication/             # Master/standby WAL streaming
    ├── status/                  # The STATUS reply
    ├── storage/                 # Storage layer
    └── wal/                     # Write-ahead log and snapshots
```

## Development

Run tests:
```bash
make test
```

Run the offline backup and restore drill against freshly built binaries:
```bash
make restore-drill
```

Run linter (`make lint-install` installs the version CI uses):
```bash
make lint
```

Smoke-test the container image (needs Docker):
```bash
make container-smoke
```

Build the release archives and checksums into `dist/` — the same ones a release publishes:
```bash
make dist VERSION=v0.0.0-dryrun
```

Verify a host archive in `dist/verify/`, including the fault checks, restore drill and short soak (Linux and GNU
coreutils):
```bash
make release-verify
```

Run the complete local gate, including Go checks and Docker smoke:
```bash
make release-check
```

For v1 RC sign-off, run the manual **Release soak** action or `make release-check SOAK_SECONDS=10800`.
Ordinary releases use the short checks. See [release verification](test/release/README.md) for coverage and limits.

Feature PRs add changelog entries under the planned next version. After merging to `main`, create the matching tag
and publish the release through GitHub; CI builds and uploads the artifacts automatically.

Clean build artifacts:
```bash
make clean
```

Generate mocks:
```bash
make generate
```

## Network Protocol

The server speaks a RESP2-style protocol over TCP (the same framing used by Redis). The CLI accepts the human-friendly
command syntax shown above and encodes it into RESP on the wire.

- **Requests** are sent as a RESP array of bulk strings, one element per token. For example, `SET users name Alice` is
  encoded as:
  ```
  *4\r\n$3\r\nSET\r\n$5\r\nusers\r\n$4\r\nname\r\n$5\r\nAlice\r\n
  ```
- **Responses** use standard RESP2 reply types, each terminated with `\r\n`:
  - Simple strings (`+OK\r\n`) for successful writes
  - Bulk strings (`$5\r\nAlice\r\n`) for values, and the null bulk string (`$-1\r\n`) for a missing key
  - Arrays (`*<n>\r\n…`) for list replies such as `TABLES` and `KEYS`
  - Integers (`:<n>\r\n`)
  - Errors (`-<CODE> <message>\r\n`), where `<CODE>` is one of the stable codes listed under
    [Error Handling](#error-handling)
- The configured `max_message_size` bounds the *requests* a server accepts and the `TABLES`/`KEYS` listings it replies
  with; either one past it is refused with `TOOLARGE`. Other replies are not bounded by the server's limit — a client
  is protected by its own, refusing to decode a larger reply (see [COMPATIBILITY.md](COMPATIBILITY.md)).
- Arguments are binary-safe: RESP framing is length-prefixed, so a value may contain spaces, CR, LF and NUL.
- Null arrays (`*-1\r\n`) are outside the supported subset — an empty result is an empty array (`*0\r\n`). The exact
  subset, and what a v1.x release promises about it, is in [COMPATIBILITY.md](COMPATIBILITY.md).

## Error Handling

Every error reply starts with a stable code token, so clients branch on the code rather than on the message text (which
is not part of the compatibility promise). The Go client exposes it as `ServerError.Code`.

| Code | Meaning |
|------|---------|
| `ERR` | unclassified failure |
| `PROTOCOL` | malformed request frame; the connection is closed after the reply |
| `UNKNOWNCMD` | no such command |
| `ARITY` | wrong number of arguments |
| `ARGUMENT` | an argument the command cannot interpret (empty table or key, unparsable literal, non-numeric `INCR` delta) |
| `TOOLARGE` | past a configured or format limit (table name, message size, a `TABLES`/`KEYS` listing, tiered storage) |
| `WRONGTYPE` | the key holds another type, or the arithmetic does not fit it |
| `READONLY` | mutation sent to a replication standby |
| `UNAVAILABLE` | the server cannot serve this in its current state |

A code a client does not recognize must be treated as `ERR`: within v1.x a code never changes meaning, but a condition
that reports `ERR` today may later be given a narrower one. See [COMPATIBILITY.md](COMPATIBILITY.md).

Beyond command errors:

- Network errors are logged and handled gracefully
- A command that panics does not take the server down. Its connection is closed without a reply, so the client
  reports the outcome as unknown (the command may have taken effect first), and the stack trace is logged. The
  storage is then fenced: data commands answer `UNAVAILABLE`, `PING` and `STATUS` keep answering and report the
  cause, and a restart recovers from disk
- Connection limit exceeded: new connections are gracefully rejected with logging

## Connection Management

The server implements connection limiting to prevent resource exhaustion:

- **Maximum Connections**: Configurable via `network.max_connections` (default: 100)
- **Connection Rejection**: When limit is reached, new connections are immediately closed
- **Graceful Handling**: Existing connections continue to work normally
- **Logging**: Connection rejections are logged with client address for monitoring
- **Resource Cleanup**: Connection slots are automatically released when clients disconnect

## Connection Pooling (Client-side)

The client supports connection pooling for master/standby deployments:

- **Multiple Servers**: Configure multiple server addresses with master/standby roles (exactly one master)
- **Read Failover**: Failed servers are temporarily excluded and retried after a timeout; reads fall back to other
  servers, while writes fail with the master down until a standby is manually promoted and clients are reconfigured
- **Admin Commands**: `PROMOTE` and `REPLICATION` are refused in pool mode — they target one specific node, so connect
  to that server directly
- **Selection Strategies**: Choose how servers are selected (master_first, round_robin, random)
- **Connection Caching**: Established connections are reused to minimize overhead
- **Concurrent Safety**: Serialized sends prevent TCP message corruption from concurrent requests
- **Configurable Retries**: Control retry attempts and delays for transient failures

## Logging

The server uses structured logging with configurable levels:
- **Debug**: Detailed request/response information
- **Info**: General operational information
- **Warn**: Warning conditions (e.g., connection limits)
- **Error**: Error conditions requiring attention

Logs can be directed to stdout or a file based on configuration.

## License

[MIT](LICENSE)
