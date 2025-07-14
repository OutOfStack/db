# Simple Database Server

A distributed key-value database with TCP server and CLI client written in Go.

## Architecture

The project consists of two main components:
- **Database Server** (`cmd/db`): TCP server that handles database operations
- **CLI Client** (`cmd/db-cli`): Command-line client for interacting with the server

## Features

- TCP-based client-server architecture
- Concurrent client handling with connection limiting
- YAML configuration support with command-line overrides
- Structured logging with configurable levels
- Graceful shutdown with proper resource cleanup
- Command-line interface for database operations
- In-memory key-value storage engine
- Connection limiting to prevent resource exhaustion

## Commands

All commands use a simple text-based protocol:

### SET
Set a key-value pair:
```
SET <key> <value>
```
Example:
```
SET user_name John
```

### GET
Get value by key:
```
GET <key>
```
Example:
```
GET user_name
```

### DEL
Delete key:
```
DEL <key>
```
Example:
```
DEL user_name
```

## Configuration

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

### Configuration Options

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

### Client Configuration

The client can be configured using a YAML file:

```yaml
network:
  address: "127.0.0.1:3223"
  max_message_size: 4
  idle_timeout: 1m
```

### Usage Examples

#### Connect with default settings:
```bash
./bin/db-cli
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
  SET key value
  GET key
  DEL key
Type 'exit' to quit

> SET name Alice
OK
> GET name
Alice
> DEL name
OK
> exit
```

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
├── cmd/                          # Command-line applications
│   ├── db/                      # Database server
│   │   └── main.go
│   └── db-cli/                  # CLI client
│       └── main.go
├── config.client.example.yaml   # Example client configuration
├── config.server.example.yaml   # Example server configuration
└── internal/                    # Internal packages
    ├── compute/                 # Request handling and command execution
    │   ├── compute.go
    │   ├── compute_test.go
    │   └── mocks/
    │       └── compute.go
    ├── config/                  # Configuration management
    │   ├── config.go           # Common configuration utilities
    │   ├── config_test.go      # Configuration tests
    │   ├── client.go           # Client configuration
    │   └── server.go           # Server configuration
    ├── engine/                  # Storage engine implementation
    │   ├── engine.go
    │   └── engine_test.go
    ├── network/                 # TCP networking layer
    │   ├── client.go           # TCP client implementation
    │   ├── server.go           # TCP server implementation
    │   └── options.go          # Functional options
    ├── parser/                  # Command parsing
    │   ├── parser.go
    │   └── parser_test.go
    └── storage/                 # Storage layer
        ├── storage.go
        ├── storage_test.go
        └── mocks/
            └── storage.go
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

## Logging

The server uses structured logging with configurable levels:
- **Debug**: Detailed request/response information
- **Info**: General operational information
- **Warn**: Warning conditions (e.g., connection limits)
- **Error**: Error conditions requiring attention

Logs can be directed to stdout or a file based on configuration.