package parser

import (
	"errors"
	"strings"
)

// maxTableNameLen is the maximum allowed length of a table name
const maxTableNameLen = 128

// Parser implements a parser for a simple key-value store
type Parser struct{}

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

	switch cmd {
	case "SET":
		if len(args) != 3 {
			return "", nil, errors.New("SET requires 3 arguments: SET <table> <key> <value>")
		}
	case "GET", "DEL":
		if len(args) != 2 {
			return "", nil, errors.New(cmd + " requires 2 arguments: " + cmd + " <table> <key>")
		}
	default:
		return "", nil, errors.New("unknown command: " + cmd)
	}

	if len(args[0]) > maxTableNameLen {
		return "", nil, errors.New("table name too long")
	}
	if args[0] == "" || args[1] == "" {
		return "", nil, errors.New("table and key cannot be empty")
	}

	return cmd, args, nil
}
