package parser

import (
	"errors"
	"fmt"
	"strings"
)

// maxTableNameLen is the maximum allowed length of a table name
const maxTableNameLen = 128

const (
	commandTables  = "TABLES"
	commandPromote = "PROMOTE"
)

// Parser implements a parser for a simple key-value store
type Parser struct{}

type commandSpec struct {
	args     int
	optional int
	readOnly bool
	// admin marks a control-plane command (e.g. replication management) whose arguments are not table-scoped and so skip
	// table/key validation.
	admin bool
	usage string
}

// commands is the central command registry used for validation and future read/write routing.
var commands = map[string]commandSpec{ //nolint:gochecknoglobals // a single registry is intentional
	"SET":          {args: 3, readOnly: false, usage: "SET <table> <key> <value>"},
	"GET":          {args: 2, readOnly: true, usage: "GET <table> <key>"},
	"DEL":          {args: 2, readOnly: false, usage: "DEL <table> <key>"},
	commandTables:  {args: 0, readOnly: true, usage: commandTables},
	"EXISTS":       {args: 1, readOnly: true, usage: "EXISTS <table>"},
	"KEYS":         {args: 1, readOnly: true, usage: "KEYS <table>"},
	"INCR":         {args: 2, optional: 1, readOnly: false, usage: "INCR <table> <key> [delta]"},
	"APPEND":       {args: 3, readOnly: false, usage: "APPEND <table> <key> <value>"},
	"HSET":         {args: 4, readOnly: false, usage: "HSET <table> <key> <field> <value>"},
	"HGET":         {args: 3, readOnly: true, usage: "HGET <table> <key> <field>"},
	"TYPE":         {args: 2, readOnly: true, usage: "TYPE <table> <key>"},
	commandPromote: {args: 0, readOnly: false, admin: true, usage: commandPromote},
	"REPLICATION":  {args: 1, readOnly: true, admin: true, usage: "REPLICATION STATUS"},
}

// IsWrite reports whether cmd mutates state and so has to be routed to a master. The pool asks this rather than keeping
// its own list: a command whose two classifications disagree is sent to a standby, refused with "ERR readonly", and not
// retried, because the caller that would fail it over believes it was a read.
//
// An unknown command is not a write — the server rejects it either way. Neither are the control-plane commands: PROMOTE
// mutates, but it is aimed at one specific node, not at whichever server currently holds the master role.
func IsWrite(cmd string) bool {
	spec, ok := commands[strings.ToUpper(strings.TrimSpace(cmd))]
	return ok && !spec.readOnly && !spec.admin
}

// IsAdmin reports whether cmd is a control-plane command aimed at one specific node (e.g. PROMOTE). The pool refuses
// to route these: it cannot promise which server a pooled command reaches.
func IsAdmin(cmd string) bool {
	spec, ok := commands[strings.ToUpper(strings.TrimSpace(cmd))]
	return ok && spec.admin
}

// IsMutation reports whether cmd changes server state, which decides whether the transport may re-send it after a
// failure. It deliberately disagrees with IsWrite on two groups, so the two must not be merged:
//
//   - control-plane commands: PROMOTE is not a "write" for routing (it targets one named node rather than whichever
//     server holds the master role) but it does mutate, so re-sending it is unsafe.
//   - unknown commands: routing can ignore them because the server rejects them anyway, whereas retry safety has to
//     assume the worst.
func IsMutation(cmd string) bool {
	spec, ok := commands[strings.ToUpper(strings.TrimSpace(cmd))]
	return !ok || !spec.readOnly
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
	if len(args) < spec.args || len(args) > spec.args+spec.optional {
		return "", nil, fmt.Errorf("%s requires %d arguments: %s", cmd, spec.args, spec.usage)
	}

	if spec.args == 0 || spec.admin {
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
