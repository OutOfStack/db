package client_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/storage"
)

// startServer starts an in-process database server on an ephemeral port
// and returns its address
func startServer(t *testing.T) string {
	t.Helper()

	logger := slog.New(slog.DiscardHandler)

	srv, err := network.NewTCPServer("127.0.0.1:0", logger)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	comp := compute.New(parser.New(), storage.New(engine.New()), logger)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go srv.Start(ctx, func(ctx context.Context, req []byte) []byte {
		res, rErr := comp.HandleRequest(ctx, string(req))
		if rErr != nil {
			return []byte(rErr.Error())
		}
		return []byte(res)
	})

	return srv.Addr().String()
}

func TestClient_RoundTrip(t *testing.T) {
	t.Parallel()

	addr := startServer(t)

	c, err := client.New(client.WithAddress(addr))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	ctx := t.Context()

	if err = c.Set(ctx, "users", "name", "vlad"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, err := c.Get(ctx, "users", "name")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != "vlad" {
		t.Errorf("Get() = %q, want %q", got, "vlad")
	}

	// same key in another table is independent
	if _, err = c.Get(ctx, "orders", "name"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("Get() from another table error = %v, want ErrNotFound", err)
	}

	if err = c.Del(ctx, "users", "name"); err != nil {
		t.Fatalf("Del() error = %v", err)
	}

	if _, err = c.Get(ctx, "users", "name"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("Get() after Del() error = %v, want ErrNotFound", err)
	}

	if err = c.Del(ctx, "users", "name"); !errors.Is(err, client.ErrNotFound) {
		t.Errorf("Del() of missing key error = %v, want ErrNotFound", err)
	}
}

func TestClient_Raw_RoundTrip(t *testing.T) {
	t.Parallel()

	addr := startServer(t)

	c, err := client.New(client.WithAddress(addr))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	ctx := t.Context()

	resp, err := c.Raw(ctx, "SET users name vlad")
	if err != nil {
		t.Fatalf("Raw(SET) error = %v", err)
	}
	if resp != "OK" {
		t.Errorf("Raw(SET) = %q, want %q", resp, "OK")
	}

	// Raw passes server errors through as text
	resp, err = c.Raw(ctx, "GET users")
	if err != nil {
		t.Fatalf("Raw(GET) error = %v", err)
	}
	if resp != "GET requires 2 arguments: GET <table> <key>" {
		t.Errorf("Raw(GET) = %q, want arity error text", resp)
	}
}

func TestClient_Pool_RoundTrip(t *testing.T) {
	t.Parallel()

	addr := startServer(t)

	c, err := client.New(
		client.WithServers(client.Server{Address: addr, Role: client.RoleMaster}),
		client.WithStrategy(client.MasterFirst),
		client.WithRetries(1, 0),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	ctx := t.Context()

	if err = c.Set(ctx, "users", "name", "vlad"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	got, err := c.Get(ctx, "users", "name")
	if err != nil || got != "vlad" {
		t.Errorf("Get() = %q, %v; want %q, nil", got, err, "vlad")
	}
}
