package parser

import (
	"errors"
	"fmt"
	"strings"
)

// maxTableNameLen is the maximum allowed length of a table name
const maxTableNameLen = 128

const commandTables = "TABLES"

// Parser implements a parser for a simple key-value store
type Parser struct{}

type commandSpec struct {
	args     int
	readOnly bool
	usage    string
}

// commands is the central command registry used for validation and future
// read/write routing.
var commands = map[string]commandSpec{ //nolint:gochecknoglobals // a single registry is intentional
	"SET":         {args: 3, readOnly: false, usage: "SET <table> <key> <value>"},
	"GET":         {args: 2, readOnly: true, usage: "GET <table> <key>"},
	"DEL":         {args: 2, readOnly: false, usage: "DEL <table> <key>"},
	commandTables: {args: 0, readOnly: true, usage: commandTables},
	"EXISTS":      {args: 1, readOnly: true, usage: "EXISTS <table>"},
	"KEYS":        {args: 1, readOnly: true, usage: "KEYS <table>"},
}

// New creates a new Parser instance.
func New() *Parser {
	return &Parser{}
}

// Parse validates a decoded command name and its arguments
func (p *Parser) Parse(cmd string, args []string) (string, []string, error) {
	cmd = strings.ToUpper(strings.TrimSpace(cmd))
	if cmd == "" {
		return "", nil, errors.New("empty input")
	}

	spec, ok := commands[cmd]
	if !ok {
		return "", nil, errors.New("unknown command: " + cmd)
	}
	if len(args) != spec.args {
		return "", nil, fmt.Errorf("%s requires %d arguments: %s", cmd, spec.args, spec.usage)
	}

	if spec.args == 0 {
		return cmd, args, nil
	}
	if len(args[0]) > maxTableNameLen {
		return "", nil, errors.New("table name too long")
	}
	if args[0] == "" {
		return "", nil, errors.New("table cannot be empty")
	}
	if spec.args >= 2 && args[1] == "" {
		return "", nil, errors.New("key cannot be empty")
	}

	return cmd, args, nil
}
