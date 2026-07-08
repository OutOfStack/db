package parser_test

import (
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/parser"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		wantCmd  string
		wantArgs []string
		wantErr  bool
	}{
		{"SET users foo bar", "SET", []string{"users", "foo", "bar"}, false},
		{"GET users foo", "GET", []string{"users", "foo"}, false},
		{"DEL users foo", "DEL", []string{"users", "foo"}, false},
		{"SET users foo", "", nil, true},
		{"SET foo bar", "", nil, true},
		{"GET foo", "", nil, true},
		{"DEL foo", "", nil, true},
		{"GET", "", nil, true},
		{"SET " + strings.Repeat("t", 129) + " foo bar", "", nil, true},
		{"UNKNOWN foo", "", nil, true},
		{"", "", nil, true},
	}

	for _, tt := range tests {
		p := parser.New()
		cmd, args, err := p.Parse(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("Parse(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if cmd != tt.wantCmd {
			t.Errorf("Parse(%q) cmd = %q, want %q", tt.input, cmd, tt.wantCmd)
		}
		if len(args) != len(tt.wantArgs) {
			t.Errorf("Parse(%q) args = %v, want %v", tt.input, args, tt.wantArgs)
			continue
		}
		for i := range args {
			if args[i] != tt.wantArgs[i] {
				t.Errorf("Parse(%q) args[%d] = %q, want %q", tt.input, i, args[i], tt.wantArgs[i])
			}
		}
	}
}
