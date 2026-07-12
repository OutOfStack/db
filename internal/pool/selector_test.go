package pool_test

import (
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/pool"
)

func TestMasterFirstSelector(t *testing.T) {
	t.Parallel()

	config := &pool.PoolConfig{
		Servers: []pool.ServerConfig{
			{Address: "master1", Role: pool.RoleMaster},
			{Address: "master2", Role: pool.RoleMaster},
			{Address: "standby1", Role: pool.RoleStandby},
			{Address: "standby2", Role: pool.RoleStandby},
		},
		FailureTimeout: time.Hour, // Long timeout for tests
	}

	selector := pool.NewMasterFirstSelector(config)

	// First select should return a master
	server := selector.Select()
	if server == nil {
		t.Fatal("Expected server, got nil")
	} else if server.Role != pool.RoleMaster {
		t.Errorf("Expected master, got %s", server.Role)
	}

	// Mark all masters as failed
	selector.MarkFailed("master1")
	selector.MarkFailed("master2")

	// Should fall back to standby
	server = selector.Select()
	if server == nil {
		t.Fatal("Expected standby server, got nil")
	} else if server.Role != pool.RoleStandby {
		t.Errorf("Expected standby, got %s", server.Role)
	}

	// Mark all standbys as failed
	selector.MarkFailed("standby1")
	selector.MarkFailed("standby2")

	// Should return nil when all servers failed
	server = selector.Select()
	if server != nil {
		t.Errorf("Expected nil when all servers failed, got %v", server)
	}

	// Reset should clear failures
	selector.Reset()
	server = selector.Select()
	if server == nil {
		t.Fatal("Expected server after reset, got nil")
	} else if server.Role != pool.RoleMaster {
		t.Errorf("Expected master after reset, got %s", server.Role)
	}
}

func TestRoundRobinSelector(t *testing.T) {
	t.Parallel()

	config := &pool.PoolConfig{
		Servers: []pool.ServerConfig{
			{Address: "server1", Role: pool.RoleMaster},
			{Address: "server2", Role: pool.RoleStandby},
			{Address: "server3", Role: pool.RoleMaster},
		},
		FailureTimeout: time.Hour, // Long timeout for tests
	}

	selector := pool.NewRoundRobinSelector(config)

	// Track which servers we get
	seen := make(map[string]int)
	for range 6 {
		server := selector.Select()
		if server == nil {
			t.Fatal("Expected server, got nil")
		}
		seen[server.Address]++
	}

	// Each server should be selected twice in round-robin
	for _, count := range seen {
		if count != 2 {
			t.Errorf("Expected each server to be selected 2 times, got %v", seen)
			break
		}
	}
}

func TestRandomSelector(t *testing.T) {
	t.Parallel()

	config := &pool.PoolConfig{
		Servers: []pool.ServerConfig{
			{Address: "server1", Role: pool.RoleMaster},
			{Address: "server2", Role: pool.RoleStandby},
			{Address: "server3", Role: pool.RoleMaster},
		},
		FailureTimeout: time.Hour, // Long timeout for tests
	}

	selector := pool.NewRandomSelector(config)

	// Select multiple times and ensure we get valid servers
	for range 10 {
		server := selector.Select()
		if server == nil {
			t.Fatal("Expected server, got nil")
		}
		// Verify it's one of our configured servers
		found := false
		for _, s := range config.Servers {
			if s.Address == server.Address {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Selected server %s not in config", server.Address)
		}
	}

	// Mark all but one server as failed
	selector.MarkFailed("server1")
	selector.MarkFailed("server2")

	// Should always return server3 now
	for range 5 {
		server := selector.Select()
		if server == nil {
			t.Fatal("Expected server3, got nil")
		}
		if server.Address != "server3" {
			t.Errorf("Expected server3, got %s", server.Address)
		}
	}
}

func TestNewSelector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy pool.SelectionStrategy
		wantType string
	}{
		{
			name:     "master first strategy",
			strategy: pool.StrategyMasterFirst,
			wantType: "*pool.MasterFirstSelector",
		},
		{
			name:     "round robin strategy",
			strategy: pool.StrategyRoundRobin,
			wantType: "*pool.RoundRobinSelector",
		},
		{
			name:     "random strategy",
			strategy: pool.StrategyRandom,
			wantType: "*pool.RandomSelector",
		},
		{
			name:     "default to master first",
			strategy: "invalid",
			wantType: "*pool.MasterFirstSelector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := &pool.PoolConfig{
				Servers: []pool.ServerConfig{
					{Address: "server1", Role: pool.RoleMaster},
				},
				SelectionStrategy: tt.strategy,
				FailureTimeout:    time.Hour, // Long timeout for tests
			}

			selector := pool.NewSelector(config)
			if selector == nil {
				t.Fatal("Expected selector, got nil")
			}

			// Type assertion to verify correct selector type
			switch tt.wantType {
			case "*pool.MasterFirstSelector":
				if _, ok := selector.(*pool.MasterFirstSelector); !ok {
					t.Errorf("Expected MasterFirstSelector, got %T", selector)
				}
			case "*pool.RoundRobinSelector":
				if _, ok := selector.(*pool.RoundRobinSelector); !ok {
					t.Errorf("Expected RoundRobinSelector, got %T", selector)
				}
			case "*pool.RandomSelector":
				if _, ok := selector.(*pool.RandomSelector); !ok {
					t.Errorf("Expected RandomSelector, got %T", selector)
				}
			}
		})
	}
}
