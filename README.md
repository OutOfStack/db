# Simple Database Server

A distributed key-value database with TCP server and CLI client written in Go.

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
- Two storage engines: `in_memory` (RAM-only) and `tiered`, whose dataset grows past RAM by keeping values in on-disk
  segments behind an LRU cache
- Tables: keys are scoped per table, created implicitly on first write
- Typed values (string, int, float, bool, array, map) with server-side atomic operations: `INCR`, `APPEND`,
  `HSET`/`HGET`, `TYPE`
- Durability: write-ahead log with `always`/`everysec`/`no` fsync policies, periodic snapshots, and crash recovery that
  truncates a torn tail
- Replication: asynchronous master/standby WAL shipping with manual `PROMOTE`
- Connection limiting to prevent resource exhaustion
- **Master/Standby Connection Pooling** with automatic failover
- Configurable server selection strategies (master_first, round_robin, random)

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
List all keys in a table in sorted order. A missing table returns an empty list. List responses are subject to the
configured client and server message size limits; pagination is not currently supported.
```
KEYS <table>
```

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
ERR wrong type: key holds array, INCR requires int or float
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
- **engine.max_storage**: MiB ceiling on live data; `SET` past it returns `ERR storage full`
- **engine.sync**: Fsync policy for segments (`always`, `everysec`, or `no`)
- **engine.segment_size**: Segment file size in MiB
- **engine.compaction_threshold**: Reclaim a sealed segment once this fraction of it is dead bytes
- **engine.compaction_interval**: How often to compact and log cache/disk stats

- **wal.enabled**: Enable durable write-ahead logging (disabled by default)
- **wal.data_dir**: Directory for WAL segments and snapshots
- **wal.sync**: Fsync policy (`always`, `everysec`, or `no`)
- **wal.segment_size**: WAL segment rollover size in MiB
- **wal.snapshot_interval**: Interval between snapshots when data has changed
- **replication.role**: `""` (standalone), `master`, or `standby`; requires `wal.enabled`
- **replication.listen_address**: Master: where standbys connect for the WAL stream. Standby: optional, so `PROMOTE` can
  start serving replication from this node
- **replication.master_address**: Standby: the master to replicate from
- **replication.reconnect_backoff**: Standby: pause between reconnect attempts
- **network.address**: Server listening address
- **network.max_connections**: Maximum concurrent client connections (enforced by server)
- **network.max_message_size**: Maximum message size in KB
- **network.idle_timeout**: Client idle timeout duration
- **logging.level**: Log level (debug, info, warn, error)
- **logging.output**: Log output file path (empty for stdout)

#### Environment Variable Overrides

Server settings can be overridden with environment variables, which take the highest priority (environment > config file
> defaults):

- `DB_ADDRESS` — listening address
- `DB_MAX_CONNECTIONS` — maximum concurrent client connections
- `DB_MAX_MESSAGE_SIZE` — maximum message size in KB
- `DB_IDLE_TIMEOUT` — client idle timeout (Go duration, e.g. `5m`)
- `DB_LOG_LEVEL` — log level
- `DB_LOG_OUTPUT` — log output file path

## Running the Server

### With default configuration:
```bash
make build
./bin/db
```

### With custom configuration:
```bash
./bin/db -config config.yaml
```

### Using make:
```bash
make run
```

### With Docker:
```bash
make docker-run
# or
docker build -t db .
docker run --rm -p 3223:3223 db
```

The image runs on default configuration with `DB_ADDRESS=0.0.0.0:3223` set, so the server is reachable through the
published port, and logs to stdout for `docker logs`. Configure it with environment variables:

```bash
docker run --rm -p 3223:3223 -e DB_LOG_LEVEL=debug -e DB_MAX_CONNECTIONS=500 db
```

Settings without an environment variable (engine type, WAL, replication) come from a YAML file. The `-config` path is
resolved relative to the working directory, which is `/home/nonroot`, so mount the file there — an absolute path is
rejected, and a path that resolves nowhere falls back to defaults silently:

```bash
docker run --rm -p 3223:3223 \
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

For distributed deployments with master/standby servers:

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
      role: master
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

The CLI also reads commands from stdin, skipping blank and `#` lines, so a prepared script runs end to end.
[`examples/commands.txt`](examples/commands.txt) covers every command:

```bash
./bin/db-cli < examples/commands.txt
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

For distributed deployments, configure a connection pool instead of a single address:

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

Error handling:
- `client.ErrNotFound` — sentinel returned by `Get`/`Del` for missing keys (check with `errors.Is`)
- `*client.ServerError` — any other error message returned by the server (check with `errors.As`)
- `Raw(ctx, command)` — escape hatch that sends a raw command line and returns the response text as is

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
├── examples/commands.txt        # Runnable command reference for the CLI
├── config.client.example.yaml   # Example client configuration
├── config.server.example.yaml   # Example server configuration
├── example-pool-config.yaml     # Example pool configuration
└── internal/                    # Internal packages
    ├── compute/                 # Request handling and command execution
    ├── config/                  # Configuration management
    ├── engine/                  # In-memory storage engine
    │   └── tiered/              # Memory/disk engine: segments, keydir, LRU, compaction
    ├── network/                 # TCP networking layer
    ├── parser/                  # Command parsing
    ├── pool/                    # Connection pooling and failover
    ├── protocol/                # RESP2 framing and the typed-value codec
    ├── replication/             # Master/standby WAL streaming
    ├── storage/                 # Storage layer
    └── wal/                     # Write-ahead log and snapshots
```

## Development

Run tests:
```bash
make test
```

Run linter:
```bash
make lint
```

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
  - Errors (`-ERR <message>\r\n`)
- Messages are bounded by the configured `max_message_size`; oversized requests are rejected.

## Error Handling

- Invalid commands return error messages
- Missing arguments return error messages
- Too many arguments return error messages
- Unknown commands return error messages
- Network errors are logged and handled gracefully
- Server implements panic recovery for client handlers
- Connection limit exceeded: new connections are gracefully rejected with logging

## Connection Management

The server implements connection limiting to prevent resource exhaustion:

- **Maximum Connections**: Configurable via `network.max_connections` (default: 100)
- **Connection Rejection**: When limit is reached, new connections are immediately closed
- **Graceful Handling**: Existing connections continue to work normally
- **Logging**: Connection rejections are logged with client address for monitoring
- **Resource Cleanup**: Connection slots are automatically released when clients disconnect

## Connection Pooling (Client-side)

The client supports connection pooling for distributed deployments:

- **Multiple Servers**: Configure multiple server addresses with master/standby roles
- **Automatic Failover**: Failed servers are temporarily excluded and automatically retried after a timeout
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
