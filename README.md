# Simple DB CLI

A simple key-value store command-line interface written in Go.

## Features

- Simple key-value storage
- Command-line interface
- JSON logging for debugging
- Error handling and user feedback
- Support for special characters in keys

## Commands

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

## Building

To build the project:
```bash
make build
```

## Running

To run the service:
```bash
make run
```

## Example Usage

The project includes an example commands file:
```bash
cat examples/commands.txt
```

## Logging

The service uses JSON logging for better observability. Logs are written to stdout and include:
- Input commands
- Error messages
- Successful command execution

## Error Handling

- Invalid commands return error messages
- Missing arguments return error messages
- Too many arguments return error messages
- Unknown commands return error messages
- Deleted keys return "not found" errors

## Exit

Type 'exit' to quit the program:
```
exit
```

## Project Structure

```
├── cmd/                    # Command-line applications
│   └── db/                # Main application
│       └── main.go
├── internal/              # Internal packages
│   ├── compute/          # Request handling and command execution
│   │   ├── compute.go
│   ├── engine/           # Storage engine implementation
│   │   ├── engine.go
│   ├── parser/           # Command parsing
│   │   ├── parser.go
│   └── storage/          # Storage layer
│       ├── storage.go
└── go.mod
```

## Development

To run tests:
```bash
make test
```

To clean up:
```bash
make clean
```
