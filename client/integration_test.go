package client_test

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
)

// startServer starts an in-process database server on an ephemeral port
// and returns its address
func startServer(t *testing.T, opts ...network.TCPServerOption) string {
	t.Helper()

	addr, _ := startStoppableServer(t, opts...)
	return addr
}

// startStoppableServer starts an in-process database server on an ephemeral
// port and returns its address and a stop function. The stop function blocks
// until the server no longer accepts new connections
func startStoppableServer(t *testing.T, opts ...network.TCPServerOption) (addr string, stop func()) {
	t.Helper()

	logger := slog.New(slog.DiscardHandler)

	srv, err := network.NewTCPServer("127.0.0.1:0", logger, opts...)
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	comp := compute.New(parser.New(), storage.New(engine.New()), logger)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go srv.Start(ctx, func(ctx context.Context, cmd string, args []string) protocol.Reply {
		res, rErr := comp.HandleRequest(ctx, cmd, args)
		if rErr != nil {
			if errors.Is(rErr, storage.ErrNotFound) {
				return protocol.NullBulkString()
			}
			return protocol.Error(rErr.Error())
		}
		return res
	})

	addr = srv.Addr().String()

	stop = func() {
		cancel()
		// wait until the listener is actually closed so new dials fail deterministically
		dialer := &net.Dialer{}
		for range 100 {
			conn, dErr := dialer.DialContext(context.Background(), "tcp", addr)
			if dErr != nil {
				return
			}
			_ = conn.Close()
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("server at %s did not stop accepting connections", addr)
	}

	return addr, stop
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

	framedValue := "hello world\nwith\x00bytes"
	if err = c.Set(ctx, "users", "framed", framedValue); err != nil {
		t.Fatalf("Set() framed value error = %v", err)
	}
	got, err = c.Get(ctx, "users", "framed")
	if err != nil {
		t.Fatalf("Get() framed value error = %v", err)
	}
	if got != framedValue {
		t.Errorf("Get() framed value = %q, want %q", got, framedValue)
	}
}

func TestClient_IntrospectionRoundTrip(t *testing.T) {
	t.Parallel()
	addr := startServer(t)
	c, err := client.New(client.WithAddress(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := t.Context()
	for _, item := range []struct{ table, key string }{{"users", "z"}, {"orders", "id"}, {"users", "a"}} {
		if err = c.Set(ctx, item.table, item.key, "v"); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := mustTables(t, c, ctx), []string{"orders", "users"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Tables() = %v, want %v", got, want)
	}
	keys, kErr := c.Keys(ctx, "users")
	if kErr != nil || !reflect.DeepEqual(keys, []string{"a", "z"}) {
		t.Fatalf("Keys() = %v, %v", keys, kErr)
	}
	exists, eErr := c.TableExists(ctx, "orders")
	if eErr != nil || !exists {
		t.Fatalf("TableExists() = %v, %v", exists, eErr)
	}
	if err = c.Del(ctx, "orders", "id"); err != nil {
		t.Fatal(err)
	}
	exists, eErr = c.TableExists(ctx, "orders")
	if eErr != nil || exists {
		t.Fatalf("TableExists() after Del = %v, %v", exists, eErr)
	}
}

func mustTables(t *testing.T, c *client.Client, ctx context.Context) []string {
	t.Helper()
	values, err := c.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return values
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

	resp, err = c.Raw(ctx, `SET users quoted "vlad has spaces"`)
	if err != nil {
		t.Fatalf("Raw(quoted SET) error = %v", err)
	}
	if resp != "OK" {
		t.Errorf("Raw(quoted SET) = %q, want OK", resp)
	}
	got, err := c.Get(ctx, "users", "quoted")
	if err != nil {
		t.Fatalf("Get(quoted) error = %v", err)
	}
	if got != "vlad has spaces" {
		t.Errorf("Get(quoted) = %q, want quoted value", got)
	}
}

func TestClient_PipelinedCommands(t *testing.T) {
	t.Parallel()

	addr := startServer(t)

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	if err = protocol.WriteCommand(conn, "SET", []string{"users", "name", "vlad"}); err != nil {
		t.Fatalf("WriteCommand(SET) error = %v", err)
	}
	if err = protocol.WriteCommand(conn, "GET", []string{"users", "name"}); err != nil {
		t.Fatalf("WriteCommand(GET) error = %v", err)
	}

	reader := bufio.NewReader(conn)
	reply, err := protocol.ReadReply(reader, 1024)
	if err != nil {
		t.Fatalf("ReadReply(SET) error = %v", err)
	}
	if reply.Kind != protocol.ReplySimpleString || reply.Value != "OK" {
		t.Fatalf("SET reply = %#v, want +OK", reply)
	}
	reply, err = protocol.ReadReply(reader, 1024)
	if err != nil {
		t.Fatalf("ReadReply(GET) error = %v", err)
	}
	if reply.Kind != protocol.ReplyBulkString || reply.Value != "vlad" {
		t.Fatalf("GET reply = %#v, want bulk vlad", reply)
	}
}

func TestClient_MessageSizeLimit(t *testing.T) {
	t.Parallel()

	const serverLimit = 1024
	addr := startServer(t, network.WithServerMaxMessageSize(serverLimit))

	c, err := client.New(client.WithAddress(addr))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	ctx := t.Context()

	// large-but-legal request near the limit round-trips
	// (frame overhead for SET users big <value> is ~40 bytes)
	nearLimit := strings.Repeat("x", serverLimit-64)
	if err = c.Set(ctx, "users", "big", nearLimit); err != nil {
		t.Fatalf("Set() near limit error = %v", err)
	}
	got, err := c.Get(ctx, "users", "big")
	if err != nil || got != nearLimit {
		t.Fatalf("Get() near limit error = %v, value match = %v", err, got == nearLimit)
	}

	// request over the limit gets a clean -ERR, not a broken connection
	err = c.Set(ctx, "users", "big", strings.Repeat("x", serverLimit*2))
	if srvErr, ok := errors.AsType[*client.ServerError](err); !ok || !strings.Contains(srvErr.Msg, "message size exceeds limit") {
		t.Fatalf("Set() over limit error = %v, want ServerError about size limit", err)
	}

	// the client stays usable afterwards
	if err = c.Set(ctx, "users", "after", "ok"); err != nil {
		t.Fatalf("Set() after rejected request error = %v", err)
	}
	if got, err = c.Get(ctx, "users", "after"); err != nil || got != "ok" {
		t.Errorf("Get() after rejected request = %q, %v; want %q, nil", got, err, "ok")
	}
}

func TestClient_OversizedReplyDoesNotDesyncConnection(t *testing.T) {
	t.Parallel()

	// server accepts large values, but the client only accepts 1KB replies
	addr := startServer(t, network.WithServerMaxMessageSize(8192))

	c, err := client.New(client.WithAddress(addr), client.WithMaxMessageSize(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	ctx := t.Context()

	big := strings.Repeat("x", 3000)
	if err = c.Set(ctx, "users", "big", big); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// the reply exceeds the client's limit and must fail...
	if _, err = c.Get(ctx, "users", "big"); err == nil || !strings.Contains(err.Error(), "message size exceeds limit") {
		t.Fatalf("Get() oversized reply error = %v, want size limit error", err)
	}

	// ...without leaving the rejected reply's bytes on the connection:
	// subsequent commands must see their own replies
	if err = c.Set(ctx, "users", "small", "v"); err != nil {
		t.Fatalf("Set() after oversized reply error = %v", err)
	}
	got, err := c.Get(ctx, "users", "small")
	if err != nil || got != "v" {
		t.Errorf("Get() after oversized reply = %q, %v; want %q, nil", got, err, "v")
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

func TestClient_Pool_Failover(t *testing.T) {
	t.Parallel()

	masterAddr, stopMaster := startStoppableServer(t)
	standbyAddr, _ := startStoppableServer(t)

	c, err := client.New(
		client.WithServers(
			client.Server{Address: masterAddr, Role: client.RoleMaster},
			client.Server{Address: standbyAddr, Role: client.RoleStandby},
		),
		client.WithStrategy(client.MasterFirst),
		client.WithRetries(3, 10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer c.Close()

	ctx := t.Context()

	// master serves writes and reads while alive
	if err = c.Set(ctx, "t", "key", "master-value"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	got, err := c.Get(ctx, "t", "key")
	if err != nil || got != "master-value" {
		t.Fatalf("Get() = %q, %v; want %q, nil", got, err, "master-value")
	}

	// Seed the standby directly (these in-process servers do not replicate) so a
	// read that fails over to it returns an observably different value.
	standby, err := client.New(client.WithAddress(standbyAddr))
	if err != nil {
		t.Fatalf("standby client New() error = %v", err)
	}
	defer standby.Close()
	if err = standby.Set(ctx, "t", "key", "standby-value"); err != nil {
		t.Fatalf("seed standby Set() error = %v", err)
	}

	stopMaster()

	// Reads fail over to the standby. The master's established connection may
	// absorb at most one more request before it closes, so retry until failover.
	if err = eventually(func() bool {
		v, gErr := c.Get(ctx, "t", "key")
		return gErr == nil && v == "standby-value"
	}); err != nil {
		t.Errorf("Get() did not fail over to standby: %v", err)
	}

	// Writes route only to masters, so with the master down they must fail rather
	// than silently land on the read-only standby.
	if err = eventually(func() bool {
		return c.Set(ctx, "t", "key", "new-value") != nil
	}); err != nil {
		t.Errorf("Set() after master stop unexpectedly succeeded (writes must not hit standbys)")
	}
}

// eventually retries cond for a short while, returning nil once it holds.
func eventually(cond func() bool) error {
	for range 50 {
		if cond() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("condition not met within timeout")
}
