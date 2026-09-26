package protocol_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/protocol"
)

// errPlain is an error carrying no code, which must report the unclassified one.
var errPlain = errors.New("plain failure")

func wrap(err error) error { return fmt.Errorf("context: %w", err) }

// TestNullArrayDecodesAsNull pins the one RESP2 shape this subset does not use. The server never writes a null array —
// an empty result is an empty array — so the decoder folds the encoding into the same null reply a null bulk string
// produces, rather than carrying a distinct type no reply can reach. A build that gave it its own kind would let a
// client branch on a distinction the server can never make.
func TestNullArrayDecodesAsNull(t *testing.T) {
	t.Parallel()

	reply, err := protocol.ReadReply(bufio.NewReader(strings.NewReader("*-1\r\n")), 0)
	if err != nil {
		t.Fatalf("ReadReply(null array) error = %v", err)
	}
	if !reflect.DeepEqual(reply, protocol.NullBulkString()) {
		t.Errorf("null array decoded as %#v, want the null reply", reply)
	}

	// Re-encoding that reply produces the null bulk string, so a null array never leaves this build.
	var buf bytes.Buffer
	if err = protocol.WriteReply(&buf, reply); err != nil {
		t.Fatalf("WriteReply(null) error = %v", err)
	}
	if buf.String() != "$-1\r\n" {
		t.Errorf("null re-encoded as %q, want %q", buf.String(), "$-1\r\n")
	}

	// An empty array stays an empty array: that is how the server reports "no keys", and it must not collapse to null.
	reply, err = protocol.ReadReply(bufio.NewReader(strings.NewReader("*0\r\n")), 0)
	if err != nil {
		t.Fatalf("ReadReply(empty array) error = %v", err)
	}
	if reply.Kind != protocol.ReplyArray || len(reply.Array) != 0 {
		t.Errorf("empty array decoded as %#v, want an empty array reply", reply)
	}
}

// TestErrorCodeRoundTrip checks the framing of coded errors in both directions, including the two edge cases a decoder
// has to get right: a message that itself starts with an upper-case word, and a reply from a server that sends no code.
func TestErrorCodeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		reply     protocol.Reply
		wire      string
		wantCode  string
		wantValue string
	}{
		{
			name:      "coded error",
			reply:     protocol.CodedError(protocol.CodeWrongType, "key holds string"),
			wire:      "-WRONGTYPE key holds string\r\n",
			wantCode:  protocol.CodeWrongType,
			wantValue: "key holds string",
		},
		{
			name:      "unclassified error",
			reply:     protocol.Error("something broke"),
			wire:      "-ERR something broke\r\n",
			wantCode:  protocol.CodeErr,
			wantValue: "something broke",
		},
		{
			name:      "message starting with an upper-case word",
			reply:     protocol.CodedError(protocol.CodeUnavailable, "PROMOTE is disabled"),
			wire:      "-UNAVAILABLE PROMOTE is disabled\r\n",
			wantCode:  protocol.CodeUnavailable,
			wantValue: "PROMOTE is disabled",
		},
		{
			name:      "empty message",
			reply:     protocol.CodedError(protocol.CodeReadOnly, ""),
			wire:      "-READONLY\r\n",
			wantCode:  protocol.CodeReadOnly,
			wantValue: "",
		},
		{
			name:      "unknown code is carried through",
			reply:     protocol.CodedError("FUTURE", "from a later server"),
			wire:      "-FUTURE from a later server\r\n",
			wantCode:  "FUTURE",
			wantValue: "from a later server",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			if err := protocol.WriteReply(&buf, tc.reply); err != nil {
				t.Fatalf("WriteReply error = %v", err)
			}
			if buf.String() != tc.wire {
				t.Errorf("encoded %q, want %q", buf.String(), tc.wire)
			}

			got, err := protocol.ReadReply(bufio.NewReader(&buf), 0)
			if err != nil {
				t.Fatalf("ReadReply error = %v", err)
			}
			if got.Code != tc.wantCode || got.Value != tc.wantValue {
				t.Errorf("decoded code/value = %q/%q, want %q/%q", got.Code, got.Value, tc.wantCode, tc.wantValue)
			}
		})
	}
}

// TestUncodedErrorReplyDecodesAsErr covers a reply whose first token is not code-shaped, which is what a server older
// than the coded-error contract sends. It has to decode as an unclassified error with the whole line intact rather than
// losing its first word.
func TestUncodedErrorReplyDecodesAsErr(t *testing.T) {
	t.Parallel()

	reply, err := protocol.ReadReply(bufio.NewReader(strings.NewReader("-wrong type: key holds string\r\n")), 0)
	if err != nil {
		t.Fatalf("ReadReply error = %v", err)
	}
	if reply.Code != protocol.CodeErr {
		t.Errorf("code = %q, want %q", reply.Code, protocol.CodeErr)
	}
	if reply.Value != "wrong type: key holds string" {
		t.Errorf("value = %q, want the whole line", reply.Value)
	}
}

// TestCodeOfWalksWrappedErrors checks that a coded error keeps its code through the wrapping the storage layers do, and
// that an error with no code reports the unclassified one.
func TestCodeOfWalksWrappedErrors(t *testing.T) {
	t.Parallel()

	coded := protocol.NewError(protocol.CodeTooLarge, "storage full")
	if got := protocol.CodeOf(coded); got != protocol.CodeTooLarge {
		t.Errorf("CodeOf(coded) = %q, want %q", got, protocol.CodeTooLarge)
	}
	if got := protocol.CodeOf(wrap(wrap(coded))); got != protocol.CodeTooLarge {
		t.Errorf("CodeOf(wrapped) = %q, want %q", got, protocol.CodeTooLarge)
	}
	if got := protocol.CodeOf(errPlain); got != protocol.CodeErr {
		t.Errorf("CodeOf(plain) = %q, want %q", got, protocol.CodeErr)
	}
	if got := protocol.CodeOf(nil); got != "" {
		t.Errorf("CodeOf(nil) = %q, want an empty code", got)
	}
}
