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
