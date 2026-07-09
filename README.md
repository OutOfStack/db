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
- In-memory key-value storage engine
- Tables: keys are scoped per table, created implicitly on first write
- Connection limiting to prevent resource exhaustion
- **Master/Standby Connection Pooling** with automatic failover
- Configurable server selection strategies (master_first, round_robin, random)

## Commands

All commands use a simple text-based protocol. Every key belongs to a table:
tables are created implicitly on the first `SET` and removed automatically when
their last key is deleted.

### SET
Set a key-value pair in a table:
```
SET <table> <key> <value>
```
Example:
```
SET users name John
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

## Configuration

### Server Configuration

The server can be configured using a YAML file. Example configuration:

```yaml
engine:
  type: "in_memory"
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

- **engine.type**: Storage engine type (currently only "in_memory")
- **network.address**: Server listening address
- **network.max_connections**: Maximum concurrent client connections (enforced by server)
- **network.max_message_size**: Maximum message size in KB
- **network.idle_timeout**: Client idle timeout duration
- **logging.level**: Log level (debug, info, warn, error)
- **logging.output**: Log output file path (empty for stdout)

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
Type 'exit' to quit

> SET users name Alice
OK
> GET users name
Alice
> DEL users name
OK
> exit
```

## Go Client Library

External Go programs can use the database through the public client package —
the only supported import path for external consumers:

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
├── config.client.example.yaml   # Example client configuration
├── config.server.example.yaml   # Example server configuration
├── example-pool-config.yaml     # Example pool configuration
└── internal/                    # Internal packages
    ├── compute/                 # Request handling and command execution
    ├── config/                  # Configuration management
    ├── engine/                  # Storage engine implementation
    ├── network/                 # TCP networking layer
    ├── parser/                  # Command parsing
    ├── pool/                    # Connection pooling and failover
    └── storage/                 # Storage layer
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

The server uses a simple text-based protocol over TCP:
- Commands are sent as plain text (same format as CLI)
- Responses are terminated with newlines
- Successful operations return "OK" or the requested value
- Errors are prefixed with "ERROR: "

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
