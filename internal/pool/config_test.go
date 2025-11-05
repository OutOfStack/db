package pool

import (
	"testing"
	"time"
)

func TestPoolConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *PoolConfig
		wantErr bool
	}{
		{
			name: "disabled pool is valid",
			config: &PoolConfig{
				Enabled: false,
			},
			wantErr: false,
		},
		{
			name: "valid pool with master and standby",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "127.0.0.1:3223", Role: RoleMaster},
					{Address: "127.0.0.1:3224", Role: RoleStandby},
				},
				SelectionStrategy: StrategyMasterFirst,
				MaxRetries:        3,
				RetryDelay:        time.Second,
			},
			wantErr: false,
		},
		{
			name: "enabled pool with no servers",
			config: &PoolConfig{
				Enabled:           true,
				Servers:           []ServerConfig{},
				SelectionStrategy: StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with no master",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "127.0.0.1:3224", Role: RoleStandby},
				},
				SelectionStrategy: StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with empty address",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "", Role: RoleMaster},
				},
				SelectionStrategy: StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with duplicate addresses",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "127.0.0.1:3223", Role: RoleMaster},
					{Address: "127.0.0.1:3223", Role: RoleStandby},
				},
				SelectionStrategy: StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with invalid role",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "127.0.0.1:3223", Role: "invalid"},
				},
				SelectionStrategy: StrategyMasterFirst,
			},
			wantErr: true,
		},
		{
			name: "pool with invalid strategy",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "127.0.0.1:3223", Role: RoleMaster},
				},
				SelectionStrategy: "invalid",
			},
			wantErr: true,
		},
		{
			name: "pool with negative max retries",
			config: &PoolConfig{
				Enabled: true,
				Servers: []ServerConfig{
					{Address: "127.0.0.1:3223", Role: RoleMaster},
				},
				SelectionStrategy: StrategyMasterFirst,
				MaxRetries:        -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("PoolConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPoolConfig_GetMasters(t *testing.T) {
	config := &PoolConfig{
		Servers: []ServerConfig{
			{Address: "127.0.0.1:3223", Role: RoleMaster},
			{Address: "127.0.0.1:3224", Role: RoleStandby},
			{Address: "127.0.0.1:3225", Role: RoleMaster},
		},
	}

	masters := config.GetMasters()
	if len(masters) != 2 {
		t.Errorf("Expected 2 masters, got %d", len(masters))
	}

	for _, m := range masters {
		if m.Role != RoleMaster {
			t.Errorf("Expected master role, got %s", m.Role)
		}
	}
}

func TestPoolConfig_GetStandbys(t *testing.T) {
	config := &PoolConfig{
		Servers: []ServerConfig{
			{Address: "127.0.0.1:3223", Role: RoleMaster},
			{Address: "127.0.0.1:3224", Role: RoleStandby},
			{Address: "127.0.0.1:3225", Role: RoleStandby},
		},
	}

	standbys := config.GetStandbys()
	if len(standbys) != 2 {
		t.Errorf("Expected 2 standbys, got %d", len(standbys))
	}

	for _, s := range standbys {
		if s.Role != RoleStandby {
			t.Errorf("Expected standby role, got %s", s.Role)
		}
	}
}
