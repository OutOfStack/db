package pool_test

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/pool"
	"github.com/OutOfStack/db/internal/protocol"
)

// startHandler runs an in-process server with a custom handler on an ephemeral port and returns its address.
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

// TestSelectWrite_OnlyMasters verifies every strategy's SelectWrite returns a master, never a standby.
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

// TestClient_WritesSkipStandbys verifies every mutating command routes only to masters while reads may use standbys.
// Round-robin is the strategy that exposes a misclassified mutation: master_first would send it to a master anyway.
func TestClient_WritesSkipStandbys(t *testing.T) {
	t.Parallel()

	mutations := []struct {
		cmd  string
		args []string
	}{
		{"SET", []string{"t", "k", "v"}},
		{"DEL", []string{"t", "k"}},
		{"INCR", []string{"t", "k"}},
		{"INCR", []string{"t", "k", "2"}},
		{"APPEND", []string{"t", "k", "v"}},
		{"HSET", []string{"t", "k", "f", "v"}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.cmd, func(t *testing.T) {
			t.Parallel()
			var masterHits, standbyHits atomic.Int32
			masterAddr := startHandler(t, okHandler(&masterHits))
			// A standby refuses a mutation, so a misrouted one is visible as a failure rather than a silent success.
			standbyAddr := startHandler(t, func(_ context.Context, _ string, _ []string) protocol.Reply {
				standbyHits.Add(1)
				return protocol.Error("readonly")
			})

			client := newPool(t, []pool.ServerConfig{
				{Address: masterAddr, Role: pool.RoleMaster},
				{Address: standbyAddr, Role: pool.RoleStandby},
			}, pool.StrategyRoundRobin)

			for range 5 {
				if _, err := client.Send(t.Context(), mutation.cmd, mutation.args); err != nil {
					t.Fatalf("Send %s: %v", mutation.cmd, err)
				}
			}
			if standbyHits.Load() != 0 {
				t.Errorf("standby received %d %s commands, want 0", standbyHits.Load(), mutation.cmd)
			}
			if masterHits.Load() != 5 {
				t.Errorf("master received %d %s commands, want 5", masterHits.Load(), mutation.cmd)
			}
		})
	}
}

// TestClient_ReadsMayUseStandbys is the other half of the rule above: a read-only command must still be free to land on
// a standby, or the pool spreads nothing.
func TestClient_ReadsMayUseStandbys(t *testing.T) {
	t.Parallel()
	var masterHits, standbyHits atomic.Int32
	masterAddr := startHandler(t, okHandler(&masterHits))
	standbyAddr := startHandler(t, okHandler(&standbyHits))

	client := newPool(t, []pool.ServerConfig{
		{Address: masterAddr, Role: pool.RoleMaster},
		{Address: standbyAddr, Role: pool.RoleStandby},
	}, pool.StrategyRoundRobin)

	for _, cmd := range []string{"GET", "TYPE", "HGET", "KEYS", "EXISTS", "TABLES"} {
		for range 4 {
			if _, err := client.Send(t.Context(), cmd, []string{"t", "k", "f"}); err != nil {
				t.Fatalf("Send %s: %v", cmd, err)
			}
		}
	}
	if standbyHits.Load() == 0 {
		t.Error("standby received no reads under round_robin")
	}
}

// TestClient_ReadOnlyFailover verifies an "ERR readonly" reply to a write is treated as a failure and retried rather
// than returned to the caller as success. With one master allowed per pool, the retries can only revisit the same
// demoted server, so Send exhausts its attempts and reports the read-only error.
func TestClient_ReadOnlyFailover(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	readOnlyAddr := startHandler(t, func(_ context.Context, _ string, _ []string) protocol.Reply {
		hits.Add(1)
		return protocol.Error("readonly")
	})

	client := newPool(t, []pool.ServerConfig{
		{Address: readOnlyAddr, Role: pool.RoleMaster},
	}, pool.StrategyMasterFirst)

	_, err := client.Send(t.Context(), "SET", []string{"t", "k", "v"})
	if err == nil {
		t.Fatal("Send SET against a read-only master succeeded, want error")
	}
	if !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("error = %v, want it to name the read-only server", err)
	}
	if hits.Load() != 4 {
		t.Errorf("read-only master hit %d times, want 4 (initial attempt + 3 retries)", hits.Load())
	}
}

// TestClient_RejectsAdminCommands verifies control-plane commands never leave the client: they target one specific
// node, and the pool cannot promise which server a routed command reaches.
func TestClient_RejectsAdminCommands(t *testing.T) {
	t.Parallel()
	var hits atomic.Int32
	masterAddr := startHandler(t, okHandler(&hits))

	client := newPool(t, []pool.ServerConfig{
		{Address: masterAddr, Role: pool.RoleMaster},
	}, pool.StrategyMasterFirst)

	for _, cmd := range []string{"PROMOTE", "REPLICATION"} {
		if _, err := client.Send(t.Context(), cmd, nil); err == nil {
			t.Errorf("Send %s through the pool succeeded, want error", cmd)
		}
	}
	if hits.Load() != 0 {
		t.Errorf("server received %d admin commands, want 0", hits.Load())
	}
}
