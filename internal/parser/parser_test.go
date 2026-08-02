package parser_test

import (
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/parser"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cmd      string
		args     []string
		wantCmd  string
		wantArgs []string
		wantErr  bool
	}{
		{"SET", []string{"users", "foo", "bar"}, "SET", []string{"users", "foo", "bar"}, false},
		{"get", []string{"users", "foo"}, "GET", []string{"users", "foo"}, false},
		{"DEL", []string{"users", "foo"}, "DEL", []string{"users", "foo"}, false},
		{"TABLES", nil, "TABLES", nil, false},
		{"exists", []string{"users"}, "EXISTS", []string{"users"}, false},
		{"KEYS", []string{"users"}, "KEYS", []string{"users"}, false},
		{"TABLES", []string{"users"}, "", nil, true},
		{"EXISTS", nil, "", nil, true},
		{"KEYS", nil, "", nil, true},
		{"SET", []string{"users", "foo"}, "", nil, true},
		{"SET", []string{"foo", "bar"}, "", nil, true},
		{"GET", []string{"foo"}, "", nil, true},
		{"DEL", []string{"foo"}, "", nil, true},
		{"GET", nil, "", nil, true},
		{"SET", []string{strings.Repeat("t", 129), "foo", "bar"}, "", nil, true},
		{"UNKNOWN", []string{"foo"}, "", nil, true},
		{"", nil, "", nil, true},
		{"GET", []string{"", "foo"}, "", nil, true},
		{"GET", []string{"users", ""}, "", nil, true},
		{"INCR", []string{"stats", "hits"}, "INCR", []string{"stats", "hits"}, false},
		{"incr", []string{"stats", "hits", "5"}, "INCR", []string{"stats", "hits", "5"}, false},
		{"INCR", []string{"stats"}, "", nil, true},
		{"INCR", []string{"stats", "hits", "5", "extra"}, "", nil, true},
		{"APPEND", []string{"t", "k", "v"}, "APPEND", []string{"t", "k", "v"}, false},
		{"APPEND", []string{"t", "k"}, "", nil, true},
		{"HSET", []string{"t", "k", "f", "v"}, "HSET", []string{"t", "k", "f", "v"}, false},
		{"HSET", []string{"t", "k", "f"}, "", nil, true},
		{"HGET", []string{"t", "k", "f"}, "HGET", []string{"t", "k", "f"}, false},
		{"TYPE", []string{"t", "k"}, "TYPE", []string{"t", "k"}, false},
		{"TYPE", []string{"t", ""}, "", nil, true},
	}

	for _, tt := range tests {
		p := parser.New()
		cmd, args, err := p.Parse(tt.cmd, tt.args)
		if (err != nil) != tt.wantErr {
			t.Errorf("Parse(%q, %q) error = %v, wantErr %v", tt.cmd, tt.args, err, tt.wantErr)
		}
		if cmd != tt.wantCmd {
			t.Errorf("Parse(%q, %q) cmd = %q, want %q", tt.cmd, tt.args, cmd, tt.wantCmd)
		}
		if len(args) != len(tt.wantArgs) {
			t.Errorf("Parse(%q, %q) args = %v, want %v", tt.cmd, tt.args, args, tt.wantArgs)
			continue
		}
		for i := range args {
			if args[i] != tt.wantArgs[i] {
				t.Errorf("Parse(%q, %q) args[%d] = %q, want %q", tt.cmd, tt.args, i, args[i], tt.wantArgs[i])
			}
		}
	}
}

// TestIsWrite pins down the classification the connection pool routes on: a mutation must reach a master, a read may go
// anywhere, and a control-plane command is aimed at one node rather than at whichever server holds the master role.
func TestIsWrite(t *testing.T) {
	tests := map[string]bool{
		"SET":         true,
		"DEL":         true,
		"INCR":        true,
		"APPEND":      true,
		"HSET":        true,
		"GET":         false,
		"HGET":        false,
		"TYPE":        false,
		"TABLES":      false,
		"EXISTS":      false,
		"KEYS":        false,
		"PROMOTE":     false,
		"REPLICATION": false,
		"NONSENSE":    false,
	}
	for cmd, want := range tests {
		if got := parser.IsWrite(cmd); got != want {
			t.Errorf("IsWrite(%q) = %v, want %v", cmd, got, want)
		}
	}

	// The RESP decoder hands the command through as the client wrote it, so classification must survive case and padding.
	if !parser.IsWrite("  set  ") {
		t.Error(`IsWrite("  set  ") = false, want true`)
	}
}
