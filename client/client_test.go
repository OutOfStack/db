package client_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/protocol"
)

type sentCommand struct {
	cmd  string
	args []string
}

// fakeTransport records sent commands and returns a canned response
type fakeTransport struct {
	resp   protocol.Reply
	err    error
	sent   []sentCommand
	closed bool
}

func (f *fakeTransport) Send(cmd string, args []string) (protocol.Reply, error) {
	f.sent = append(f.sent, sentCommand{cmd: cmd, args: append([]string(nil), args...)})
	if f.err != nil {
		return protocol.Reply{}, f.err
	}
	return f.resp, nil
}

func (f *fakeTransport) Close() error {
	f.closed = true
	return nil
}

func TestClient_Set(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resp      protocol.Reply
		sendErr   error
		wantErr   bool
		wantSrvEr bool
	}{
		{"ok", protocol.SimpleString("OK"), nil, false, false},
		{"server error", protocol.Error("table name too long"), nil, true, true},
		{"unexpected response", protocol.BulkString("weird"), nil, true, true},
		{"transport error", protocol.Reply{}, errors.New("conn refused"), true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ft := &fakeTransport{resp: tt.resp, err: tt.sendErr}
			c := client.NewWithTransport(ft)

			err := c.Set(t.Context(), "users", "name", "vlad")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Set() error = %v, wantErr %v", err, tt.wantErr)
			}
			var srvErr *client.ServerError
			if errors.As(err, &srvErr) != tt.wantSrvEr {
				t.Errorf("Set() ServerError = %v, want %v", err, tt.wantSrvEr)
			}
			wantSent := []sentCommand{{cmd: "SET", args: []string{"users", "name", "vlad"}}}
			if !reflect.DeepEqual(ft.sent, wantSent) {
				t.Errorf("sent = %#v, want %#v", ft.sent, wantSent)
			}
		})
	}
}

func TestClient_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resp    protocol.Reply
		want    string
		wantErr error
	}{
		{"value", protocol.BulkString("vlad"), "vlad", nil},
		{"value with trailing newline", protocol.BulkString("vlad\n"), "vlad\n", nil},
		{"not found", protocol.NullBulkString(), "", client.ErrNotFound},
		{"server error", protocol.Error("bad"), "", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := client.NewWithTransport(&fakeTransport{resp: tt.resp})

			got, err := c.Get(t.Context(), "users", "name")
			if tt.resp.Kind == protocol.ReplyError {
				if _, ok := errors.AsType[*client.ServerError](err); !ok {
					t.Fatalf("Get() error = %v, want ServerError", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Get() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Get() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClient_Del(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resp      protocol.Reply
		wantErr   error
		wantSrvEr bool
	}{
		{"ok", protocol.SimpleString("OK"), nil, false},
		{"not found", protocol.NullBulkString(), client.ErrNotFound, false},
		{"server error", protocol.Error("some error"), nil, true},
		{"unexpected response", protocol.BulkString("some error"), nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := client.NewWithTransport(&fakeTransport{resp: tt.resp})

			err := c.Del(t.Context(), "users", "name")
			var srvErr *client.ServerError
			if errors.As(err, &srvErr) {
				if !tt.wantSrvEr {
					t.Fatalf("Del() unexpected ServerError: %v", err)
				}
				return
			}
			if tt.wantSrvEr {
				t.Fatalf("Del() error = %v, want ServerError", err)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Del() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Raw(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{resp: protocol.NullBulkString()}
	c := client.NewWithTransport(ft)

	// Raw passes the response through without error mapping
	got, err := c.Raw(t.Context(), `GET users "missing key"`)
	if err != nil {
		t.Fatalf("Raw() error = %v", err)
	}
	if got != "not found" {
		t.Errorf("Raw() = %q, want %q", got, "not found")
	}
	wantSent := []sentCommand{{cmd: "GET", args: []string{"users", "missing key"}}}
	if !reflect.DeepEqual(ft.sent, wantSent) {
		t.Errorf("sent = %#v, want %#v", ft.sent, wantSent)
	}

	ft.resp = protocol.BulkString("hello\nworld")
	got, err = c.Raw(t.Context(), `GET users hello\nworld`)
	if err != nil {
		t.Fatalf("Raw() with escape error = %v", err)
	}
	if got != "hello\nworld" {
		t.Errorf("Raw() escaped response = %q, want newline value", got)
	}

	if _, err = c.Raw(t.Context(), "   "); err == nil {
		t.Error("Raw() with empty command should fail")
	}
	if _, err = c.Raw(t.Context(), `SET t k "unterminated`); err == nil {
		t.Error("Raw() with unterminated quote should fail")
	}
	if _, err = c.Raw(t.Context(), `SET t k trailing\`); err == nil {
		t.Error("Raw() with unfinished escape should fail")
	}
	if len(ft.sent) != 2 {
		t.Errorf("rejected commands must not reach the transport, sent: %#v", ft.sent)
	}
}

func TestClient_ArgValidation(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{resp: protocol.SimpleString("OK")}
	c := client.NewWithTransport(ft)

	tests := []struct {
		name string
		call func() error
	}{
		{"empty table", func() error { return c.Set(t.Context(), "", "k", "v") }},
		{"empty key", func() error { _, err := c.Get(t.Context(), "t", ""); return err }},
		{"table too long", func() error { return c.Set(t.Context(), strings.Repeat("t", 129), "k", "v") }},
	}

	for _, tt := range tests {
		if err := tt.call(); err == nil {
			t.Errorf("%s: expected validation error, got nil", tt.name)
		}
	}
	if len(ft.sent) != 0 {
		t.Errorf("invalid arguments must not reach the transport, sent: %#v", ft.sent)
	}
}

func TestClient_AllowsFramedValues(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{resp: protocol.SimpleString("OK")}
	c := client.NewWithTransport(ft)

	value := "a b\nc\x00"
	if err := c.Set(t.Context(), "t", "k", value); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	wantSent := []sentCommand{{cmd: "SET", args: []string{"t", "k", value}}}
	if !reflect.DeepEqual(ft.sent, wantSent) {
		t.Errorf("sent = %#v, want %#v", ft.sent, wantSent)
	}
}

func TestClient_ContextCanceled(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{resp: protocol.SimpleString("OK")}
	c := client.NewWithTransport(ft)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := c.Set(ctx, "t", "k", "v"); !errors.Is(err, context.Canceled) {
		t.Errorf("Set() error = %v, want context.Canceled", err)
	}
	if len(ft.sent) != 0 {
		t.Errorf("canceled context must not reach the transport, sent: %#v", ft.sent)
	}
}

func TestClient_Close(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{}
	c := client.NewWithTransport(ft)

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !ft.closed {
		t.Error("Close() did not close the transport")
	}
}

func TestNew_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []client.Option
	}{
		{"empty address", []client.Option{client.WithAddress("")}},
		{"servers without master", []client.Option{
			client.WithServers(client.Server{Address: "a:1", Role: client.RoleStandby}),
		}},
		{"invalid role", []client.Option{
			client.WithServers(client.Server{Address: "a:1", Role: "bogus"}),
		}},
		{"invalid strategy", []client.Option{
			client.WithServers(client.Server{Address: "a:1", Role: client.RoleMaster}),
			client.WithStrategy("bogus"),
		}},
		{"negative retries", []client.Option{
			client.WithServers(client.Server{Address: "a:1", Role: client.RoleMaster}),
			client.WithRetries(-1, 0),
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := client.New(tt.opts...); err == nil {
				t.Error("New() expected error, got nil")
			}
		})
	}
}
