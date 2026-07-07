package pool_test

import (
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/pool"
)

func TestPoolConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  *pool.PoolConfig
		wantErr bool
	}{
		{
			name: "disabled pool is valid",
			config: &pool.PoolConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "valid pool with master and standby",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "127.0.0.1:3223", Role: pool.RoleMaster},
					{Address: "127.0.0.1:3224", Role: pool.RoleStandby},
				},
				SelectionStrategy: pool.StrategyMasterFirst,
				MaxRetries:        3,
				RetryDelay:        time.Second,
			},
			wantErr: false,
		},
		{
			name: "enabled pool with no servers",
			config: &pool.PoolConfig{
				Enabled:           true,
				Servers:           []pool.ServerConfig{},
				SelectionStrategy: pool.StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with no master",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "127.0.0.1:3224", Role: pool.RoleStandby},
				},
				SelectionStrategy: pool.StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with empty address",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "", Role: pool.RoleMaster},
				},
				SelectionStrategy: pool.StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with duplicate addresses",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "127.0.0.1:3223", Role: pool.RoleMaster},
					{Address: "127.0.0.1:3223", Role: pool.RoleStandby},
				},
				SelectionStrategy: pool.StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with invalid role",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "127.0.0.1:3223", Role: "invalid"},
				},
				SelectionStrategy: pool.StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with invalid strategy",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "127.0.0.1:3223", Role: pool.RoleMaster},
				},
				SelectionStrategy: "invalid",
			},
			wantErr: true,
		},
		{
			name: "pool with negative max retries",
			config: &pool.PoolConfig{
				Enabled: true,
				Servers: []pool.ServerConfig{
					{Address: "127.0.0.1:3223", Role: pool.RoleMaster},
				},
				SelectionStrategy: pool.StrategyMasterFirst,
				MaxRetries:        -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("PoolConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPoolConfig_GetMasters(t *testing.T) {
	t.Parallel()

	config := &pool.PoolConfig{
		Servers: []pool.ServerConfig{
			{Address: "127.0.0.1:3223", Role: pool.RoleMaster},
			{Address: "127.0.0.1:3224", Role: pool.RoleStandby},
			{Address: "127.0.0.1:3225", Role: pool.RoleMaster},
		},
	}

	masters := config.GetMasters()
	if len(masters) != 2 {
		t.Errorf("Expected 2 masters, got %d", len(masters))
	}

	for _, m := range masters {
		if m.Role != pool.RoleMaster {
			t.Errorf("Expected master role, got %s", m.Role)
		}
	}
}

func TestPoolConfig_GetStandbys(t *testing.T) {
	t.Parallel()

	config := &pool.PoolConfig{
		Servers: []pool.ServerConfig{
			{Address: "127.0.0.1:3223", Role: pool.RoleMaster},
			{Address: "127.0.0.1:3224", Role: pool.RoleStandby},
			{Address: "127.0.0.1:3225", Role: pool.RoleStandby},
		},
	}

	standbys := config.GetStandbys()
	if len(standbys) != 2 {
		t.Errorf("Expected 2 standbys, got %d", len(standbys))
	}

	for _, s := range standbys {
		if s.Role != pool.RoleStandby {
			t.Errorf("Expected standby role, got %s", s.Role)
		}
	}
}
