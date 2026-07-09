package client_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/OutOfStack/db/client"
)

// fakeTransport records sent data and returns a canned response
type fakeTransport struct {
	resp   string
	err    error
	sent   []string
	closed bool
}

func (f *fakeTransport) Send(data []byte) ([]byte, error) {
	f.sent = append(f.sent, string(data))
	if f.err != nil {
		return nil, f.err
	}
	return []byte(f.resp), nil
}

func (f *fakeTransport) Close() error {
	f.closed = true
	return nil
}

func TestClient_Set(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		resp      string
		sendErr   error
		wantSent  string
		wantErr   bool
		wantSrvEr bool
	}{
		{"ok", "OK", nil, "SET users name vlad\n", false, false},
		{"server error", "table name too long", nil, "SET users name vlad\n", true, true},
		{"transport error", "", errors.New("conn refused"), "SET users name vlad\n", true, false},
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
			if len(ft.sent) != 1 || ft.sent[0] != tt.wantSent {
				t.Errorf("sent = %q, want %q", ft.sent, tt.wantSent)
			}
		})
	}
}

func TestClient_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		resp    string
		want    string
		wantErr error
	}{
		{"value", "vlad", "vlad", nil},
		{"value with trailing newline", "vlad\n", "vlad", nil},
		{"not found", "not found", "", client.ErrNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := client.NewWithTransport(&fakeTransport{resp: tt.resp})

			got, err := c.Get(t.Context(), "users", "name")
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
		resp      string
		wantErr   error
		wantSrvEr bool
	}{
		{"ok", "OK", nil, false},
		{"not found", "not found", client.ErrNotFound, false},
		{"server error", "some error", nil, true},
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

	ft := &fakeTransport{resp: "not found\n"}
	c := client.NewWithTransport(ft)

	// Raw passes the response through without error mapping
	got, err := c.Raw(t.Context(), "GET users missing")
	if err != nil {
		t.Fatalf("Raw() error = %v", err)
	}
	if got != "not found" {
		t.Errorf("Raw() = %q, want %q", got, "not found")
	}
	if ft.sent[0] != "GET users missing\n" {
		t.Errorf("sent = %q, want %q", ft.sent[0], "GET users missing\n")
	}

	if _, err = c.Raw(t.Context(), "   "); err == nil {
		t.Error("Raw() with empty command should fail")
	}
	if _, err = c.Raw(t.Context(), "SET t k v\nDEL t k"); err == nil {
		t.Error("Raw() with embedded newline should fail")
	}
	// trailing newlines are trimmed, only embedded ones are rejected
	if _, err = c.Raw(t.Context(), "GET t\rk"); err == nil {
		t.Error("Raw() with embedded carriage return should fail")
	}
	if len(ft.sent) != 1 {
		t.Errorf("rejected commands must not reach the transport, sent: %q", ft.sent)
	}
}

func TestClient_ArgValidation(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{resp: "OK"}
	c := client.NewWithTransport(ft)

	tests := []struct {
		name string
		call func() error
	}{
		{"empty table", func() error { return c.Set(t.Context(), "", "k", "v") }},
		{"empty key", func() error { _, err := c.Get(t.Context(), "t", ""); return err }},
		{"key with space", func() error { return c.Del(t.Context(), "t", "a b") }},
		{"value with space", func() error { return c.Set(t.Context(), "t", "k", "a b") }},
		{"value with tab", func() error { return c.Set(t.Context(), "t", "k", "a\tb") }},
		{"value with newline", func() error { return c.Set(t.Context(), "t", "k", "a\nb") }},
		{"table too long", func() error { return c.Set(t.Context(), strings.Repeat("t", 129), "k", "v") }},
	}

	for _, tt := range tests {
		if err := tt.call(); err == nil {
			t.Errorf("%s: expected validation error, got nil", tt.name)
		}
	}
	if len(ft.sent) != 0 {
		t.Errorf("invalid arguments must not reach the transport, sent: %q", ft.sent)
	}
}

func TestClient_ContextCanceled(t *testing.T) {
	t.Parallel()

	ft := &fakeTransport{resp: "OK"}
	c := client.NewWithTransport(ft)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := c.Set(ctx, "t", "k", "v"); !errors.Is(err, context.Canceled) {
		t.Errorf("Set() error = %v, want context.Canceled", err)
	}
	if len(ft.sent) != 0 {
		t.Errorf("canceled context must not reach the transport, sent: %q", ft.sent)
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
