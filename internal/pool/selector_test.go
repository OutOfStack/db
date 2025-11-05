package pool

import (
	"testing"
)

func TestMasterFirstSelector(t *testing.T) {
	config := &PoolConfig{
		Servers: []ServerConfig{
			{Address: "master1", Role: RoleMaster},
			{Address: "master2", Role: RoleMaster},
			{Address: "standby1", Role: RoleStandby},
			{Address: "standby2", Role: RoleStandby},
		},
	}

	selector := NewMasterFirstSelector(config)

	// First select should return a master
	server := selector.Select()
	if server == nil {
		t.Fatal("Expected server, got nil")
	}
	if server.Role != RoleMaster {
		t.Errorf("Expected master, got %s", server.Role)
	}

	// Mark all masters as failed
	selector.MarkFailed("master1")
	selector.MarkFailed("master2")

	// Should fall back to standby
	server = selector.Select()
	if server == nil {
		t.Fatal("Expected standby server, got nil")
	}
	if server.Role != RoleStandby {
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
	}
	if server.Role != RoleMaster {
		t.Errorf("Expected master after reset, got %s", server.Role)
	}
}

func TestRoundRobinSelector(t *testing.T) {
	config := &PoolConfig{
		Servers: []ServerConfig{
			{Address: "server1", Role: RoleMaster},
			{Address: "server2", Role: RoleStandby},
			{Address: "server3", Role: RoleMaster},
		},
	}

	selector := NewRoundRobinSelector(config)

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
	config := &PoolConfig{
		Servers: []ServerConfig{
			{Address: "server1", Role: RoleMaster},
			{Address: "server2", Role: RoleStandby},
			{Address: "server3", Role: RoleMaster},
		},
	}

	selector := NewRandomSelector(config)

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
	tests := []struct {
		name     string
		strategy SelectionStrategy
		wantType string
	}{
		{
			name:     "master first strategy",
			strategy: StrategyMasterFirst,
			wantType: "*pool.MasterFirstSelector",
		},
		{
			name:     "round robin strategy",
			strategy: StrategyRoundRobin,
			wantType: "*pool.RoundRobinSelector",
		},
		{
			name:     "random strategy",
			strategy: StrategyRandom,
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
			config := &PoolConfig{
				Servers: []ServerConfig{
					{Address: "server1", Role: RoleMaster},
				},
				SelectionStrategy: tt.strategy,
			}

			selector := NewSelector(config)
			if selector == nil {
				t.Fatal("Expected selector, got nil")
			}

			// Type assertion to verify correct selector type
			switch tt.wantType {
			case "*pool.MasterFirstSelector":
				if _, ok := selector.(*MasterFirstSelector); !ok {
					t.Errorf("Expected MasterFirstSelector, got %T", selector)
				}
			case "*pool.RoundRobinSelector":
				if _, ok := selector.(*RoundRobinSelector); !ok {
					t.Errorf("Expected RoundRobinSelector, got %T", selector)
				}
			case "*pool.RandomSelector":
				if _, ok := selector.(*RandomSelector); !ok {
					t.Errorf("Expected RandomSelector, got %T", selector)
				}
			}
		})
	}
}
