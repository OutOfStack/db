//go:generate mockgen -destination=mocks/mock_parser.go -package=mocks . Parser

package main

import (
	"errors"
	"strings"
)

// Parser defines the interface for parsing queries.
type Parser interface {
	// Parse parses the input string and returns the command, arguments, and an error if parsing fails.
	Parse(input string) (cmd string, args []string, err error)
}

type parser struct{}

// NewParser creates a new Parser instance.
func NewParser() Parser {
	return &parser{}
}

// Parse parses the input string and returns the command, arguments, and an error if parsing fails.
func (p *parser) Parse(input string) (string, []string, error) {
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
