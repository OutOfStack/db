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

// Parse parses the input string and returns the command and arguments
func (p *Parser) Parse(input string) (string, []string, error) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return "", nil, errors.New("empty input")
	}

	cmd := fields[0]
	args := fields[1:]

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

	return cmd, args, nil
}
