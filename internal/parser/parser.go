package parser

import (
	"errors"
	"strings"
)

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
		if len(args) != 2 {
			return "", nil, errors.New("SET requires 2 arguments")
		}
	case "GET", "DEL":
		if len(args) != 1 {
			return "", nil, errors.New(cmd + " requires 1 argument")
		}
	default:
		return "", nil, errors.New("unknown command: " + cmd)
	}

	return cmd, args, nil
}
