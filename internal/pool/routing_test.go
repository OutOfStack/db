package pool_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/pool"
	"github.com/OutOfStack/db/internal/protocol"
)

// startHandler runs an in-process server with a custom handler on an ephemeral
// port and returns its address.
func startHandler(t *testing.T, handler network.RequestHandler) string {
	t.Helper()
	srv, err := network.NewTCPServer("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewTCPServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go srv.Start(ctx, handler)
	return srv.Addr().String()
}

func okHandler(hits *atomic.Int32) network.RequestHandler {
	return func(_ context.Context, _ string, _ []string) protocol.Reply {
		hits.Add(1)
		return protocol.SimpleString("OK")
	}
}

func newPool(t *testing.T, servers []pool.ServerConfig, strategy pool.SelectionStrategy) *pool.Client {
	t.Helper()
	client, err := pool.NewClient(&pool.PoolConfig{
		Enabled:           true,
		Servers:           servers,
		SelectionStrategy: strategy,
		MaxRetries:        3,
		RetryDelay:        5 * time.Millisecond,
		FailureTimeout:    time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestSelectWrite_OnlyMasters verifies every strategy's SelectWrite returns a
// master, never a standby.
func TestSelectWrite_OnlyMasters(t *testing.T) {
	t.Parallel()
	config := &pool.PoolConfig{
		Servers: []pool.ServerConfig{
			{Address: "m1", Role: pool.RoleMaster},
			{Address: "s1", Role: pool.RoleStandby},
			{Address: "s2", Role: pool.RoleStandby},
		},
		FailureTimeout: time.Hour,
	}
	for _, strategy := range []pool.SelectionStrategy{pool.StrategyMasterFirst, pool.StrategyRoundRobin, pool.StrategyRandom} {
		config.SelectionStrategy = strategy
		selector := pool.NewSelector(config)
		for range 20 {
			server := selector.SelectWrite()
			if server == nil {
				t.Fatalf("%s: SelectWrite returned nil", strategy)
			}
			if server.Role != pool.RoleMaster {
				t.Fatalf("%s: SelectWrite returned %s (role %s), want a master", strategy, server.Address, server.Role)
			}
		}
	}
}

// TestClient_WritesSkipStandbys verifies writes route only to masters while
// reads may use standbys.
func TestClient_WritesSkipStandbys(t *testing.T) {
	t.Parallel()
	var masterHits, standbyHits atomic.Int32
	masterAddr := startHandler(t, okHandler(&masterHits))
	standbyAddr := startHandler(t, okHandler(&standbyHits))

	client := newPool(t, []pool.ServerConfig{
		{Address: masterAddr, Role: pool.RoleMaster},
		{Address: standbyAddr, Role: pool.RoleStandby},
	}, pool.StrategyMasterFirst)

	for range 5 {
		if _, err := client.Send("SET", []string{"t", "k", "v"}); err != nil {
			t.Fatalf("Send SET: %v", err)
		}
	}
	if standbyHits.Load() != 0 {
		t.Errorf("standby received %d writes, want 0", standbyHits.Load())
	}
	if masterHits.Load() != 5 {
		t.Errorf("master received %d writes, want 5", masterHits.Load())
	}
}

// TestClient_ReadOnlyFailover verifies an "ERR readonly" reply to a write marks
// the server stale and reroutes to another master.
func TestClient_ReadOnlyFailover(t *testing.T) {
	t.Parallel()
	var goodHits atomic.Int32
	readOnlyAddr := startHandler(t, func(_ context.Context, _ string, _ []string) protocol.Reply {
		return protocol.Error("readonly")
	})
	goodAddr := startHandler(t, okHandler(&goodHits))

	client := newPool(t, []pool.ServerConfig{
		{Address: readOnlyAddr, Role: pool.RoleMaster},
		{Address: goodAddr, Role: pool.RoleMaster},
	}, pool.StrategyMasterFirst)

	resp, err := client.Send("SET", []string{"t", "k", "v"})
	if err != nil {
		t.Fatalf("Send SET: %v", err)
	}
	if resp.Kind != protocol.ReplySimpleString || resp.Value != "OK" {
		t.Fatalf("reply = %+v, want +OK", resp)
	}
	if goodHits.Load() == 0 {
		t.Error("write never reached the writable master after readonly failover")
	}
}
