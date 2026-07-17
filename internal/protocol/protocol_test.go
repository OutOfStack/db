package protocol_test

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/OutOfStack/db/internal/protocol"
)

func TestCommandRoundTrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	args := []string{"users", "name", "vlad has\nbytes\x00"}

	if err := protocol.WriteCommand(&buf, "SET", args); err != nil {
		t.Fatalf("WriteCommand() error = %v", err)
	}

	cmd, gotArgs, err := protocol.ReadCommand(bufio.NewReader(&buf), 1024)
	if err != nil {
		t.Fatalf("ReadCommand() error = %v", err)
	}
	if cmd != "SET" {
		t.Errorf("cmd = %q, want SET", cmd)
	}
	if !reflect.DeepEqual(gotArgs, args) {
		t.Errorf("args = %#v, want %#v", gotArgs, args)
	}
}

func TestCommandSizeMatchesWriteCommand(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cmd  string
		args []string
	}{
		{"SET", []string{"users", "name", "vlad"}},
		{"DEL", []string{"users", "name"}},
		{"SET", []string{"t", "k", ""}},
		{"SET", []string{"table", "key", "a value spanning multiple words and \r\n bytes"}},
	}

	for _, c := range cases {
		var buf bytes.Buffer
		if err := protocol.WriteCommand(&buf, c.cmd, c.args); err != nil {
			t.Fatalf("WriteCommand() error = %v", err)
		}
		if got := protocol.CommandSize(c.cmd, c.args); got != buf.Len() {
			t.Errorf("CommandSize(%q, %v) = %d, want %d", c.cmd, c.args, got, buf.Len())
		}
	}
}

func TestReadCommandRejectsMalformedFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"plain text", "GET users name\r\n"},
		{"zero args", "*0\r\n"},
		{"non bulk arg", "*1\r\n+GET\r\n"},
		{"missing crlf", "*1\n$3\r\nGET\r\n"},
		{"null command arg", "*1\r\n$-1\r\n"},
		{"bad bulk terminator", "*1\r\n$3\r\nGET\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := protocol.ReadCommand(bufio.NewReader(bytes.NewBufferString(tt.input)), 1024)
			if err == nil {
				t.Fatal("ReadCommand() expected error, got nil")
			}
		})
	}
}

func TestReadCommandTruncatedFrame(t *testing.T) {
	t.Parallel()

	_, _, err := protocol.ReadCommand(bufio.NewReader(bytes.NewBufferString("*2\r\n$3\r\nGET\r\n$5\r\nus")), 1024)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ReadCommand() error = %v, want unexpected EOF", err)
	}
}

func TestReadCommandSizeLimit(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	if err := protocol.WriteCommand(&buf, "SET", []string{"t", "k", "0123456789"}); err != nil {
		t.Fatalf("WriteCommand() error = %v", err)
	}

	_, _, err := protocol.ReadCommand(bufio.NewReader(&buf), 20)
	if err == nil {
		t.Fatal("ReadCommand() expected size error, got nil")
	}
}

func TestReplyRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []protocol.Reply{
		protocol.SimpleString("OK"),
		protocol.BulkString("vlad has\nbytes\x00"),
		protocol.NullBulkString(),
		protocol.Error("bad command"),
		protocol.Integer(42),
		protocol.BulkStringArray([]string{"users", "orders"}),
	}

	for _, want := range tests {
		var buf bytes.Buffer
		if err := protocol.WriteReply(&buf, want); err != nil {
			t.Fatalf("WriteReply(%#v) error = %v", want, err)
		}
		got, err := protocol.ReadReply(bufio.NewReader(&buf), 1024)
		if err != nil {
			t.Fatalf("ReadReply(%#v) error = %v", want, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("reply = %#v, want %#v", got, want)
		}
	}
}

func TestReadReplyRejectsMalformedPrefix(t *testing.T) {
	t.Parallel()

	_, err := protocol.ReadReply(bufio.NewReader(bytes.NewBufferString("!wat\r\n")), 1024)
	if err == nil {
		t.Fatal("ReadReply() expected error, got nil")
	}
}

func TestRejectsHugeArrayCount(t *testing.T) {
	t.Parallel()

	// declared counts that could never fit in the size limit must be rejected
	// before any allocation happens (a naive prealloc panics or OOMs)
	for _, input := range []string{
		"*9223372036854775807\r\n",
		"*1000000000\r\n$1\r\na\r\n",
	} {
		if _, _, err := protocol.ReadCommand(bufio.NewReader(bytes.NewBufferString(input)), 1024); err == nil {
			t.Errorf("ReadCommand(%q) expected error, got nil", input)
		}
		if _, err := protocol.ReadReply(bufio.NewReader(bytes.NewBufferString(input)), 1024); err == nil {
			t.Errorf("ReadReply(%q) expected error, got nil", input)
		}
	}
}

func TestWriteReplySanitizesCRLF(t *testing.T) {
	t.Parallel()

	// CR/LF in line-based replies must not break framing: a crafted error
	// message must decode as exactly one reply, not smuggle in a second one
	var buf bytes.Buffer
	if err := protocol.WriteReply(&buf, protocol.Error("unknown command: FOO\r\n+OK")); err != nil {
		t.Fatalf("WriteReply() error = %v", err)
	}
	reader := bufio.NewReader(&buf)
	got, err := protocol.ReadReply(reader, 1024)
	if err != nil {
		t.Fatalf("ReadReply() error = %v", err)
	}
	if got.Kind != protocol.ReplyError || got.Value != "unknown command: FOO  +OK" {
		t.Errorf("reply = %#v, want sanitized error", got)
	}
	if _, err = protocol.ReadReply(reader, 1024); !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF after single reply, got %v", err)
	}
}

func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	f.Add([]byte("*9223372036854775807\r\n"))
	f.Add([]byte("*1\r\n$-1\r\n"))
	f.Add([]byte("GET users name\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		cmd, args, err := protocol.ReadCommand(bufio.NewReader(bytes.NewReader(data)), 1024)
		if err != nil {
			return
		}
		// whatever decodes must re-encode and decode to the same command
		var buf bytes.Buffer
		if err = protocol.WriteCommand(&buf, cmd, args); err != nil {
			t.Fatalf("WriteCommand(%q, %q) error = %v", cmd, args, err)
		}
		cmd2, args2, err := protocol.ReadCommand(bufio.NewReader(&buf), 1024)
		if err != nil {
			t.Fatalf("re-decode of %q %q error = %v", cmd, args, err)
		}
		if cmd2 != cmd || !reflect.DeepEqual(args2, args) {
			t.Fatalf("round trip = %q %q, want %q %q", cmd2, args2, cmd, args)
		}
	})
}

func FuzzReadReply(f *testing.F) {
	f.Add([]byte("+OK\r\n"))
	f.Add([]byte("-ERR bad command\r\n"))
	f.Add([]byte(":42\r\n"))
	f.Add([]byte("$-1\r\n"))
	f.Add([]byte("*2\r\n$5\r\nusers\r\n$6\r\norders\r\n"))
	f.Add([]byte("*1000000000\r\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		r1, err := protocol.ReadReply(bufio.NewReader(bytes.NewReader(data)), 1024)
		if err != nil {
			return
		}
		// the first encode may normalize (ERR prefix, CR/LF sanitization);
		// after that, encode/decode must be a stable round trip
		r2, err := rewrite(r1)
		if err != nil {
			t.Fatalf("first re-encode of %#v error = %v", r1, err)
		}
		r3, err := rewrite(r2)
		if err != nil {
			t.Fatalf("second re-encode of %#v error = %v", r2, err)
		}
		if !reflect.DeepEqual(r2, r3) {
			t.Fatalf("round trip not stable: %#v != %#v", r2, r3)
		}
	})
}

// rewrite encodes a reply and decodes it back
func rewrite(reply protocol.Reply) (protocol.Reply, error) {
	var buf bytes.Buffer
	if err := protocol.WriteReply(&buf, reply); err != nil {
		return protocol.Reply{}, err
	}
	return protocol.ReadReply(bufio.NewReader(&buf), 1<<20)
}
