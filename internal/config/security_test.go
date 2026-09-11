package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/stretchr/testify/require"
)

func TestListenAddressesRequireRemoteOptIn(t *testing.T) {
	t.Parallel()
	addresses := []struct {
		address  string
		loopback bool
	}{
		{"127.0.0.1:3223", true},
		{"127.10.20.30:3223", true},
		{"[::1]:3223", true},
		{"[::ffff:127.0.0.1]:3223", true},
		{"0.0.0.0:3223", false},
		{"8.8.8.8:3223", false},
		{"192.168.1.2:3223", false},
		{"[::]:3223", false},
		{"[fe80::1%eth0]:3223", false},
		{":3223", false},
		{"localhost:3223", false},
		{"db.example:3223", false},
	}
	for _, address := range addresses {
		t.Run(address.address, func(t *testing.T) {
			t.Parallel()
			for _, target := range []string{"client", "master", "promotion"} {
				for _, allow := range []bool{false, true} {
					cfg := config.DefaultServerConfig()
					cfg.Network.AllowRemote = allow
					if target == "client" {
						cfg.Network.Address = address.address
					} else {
						cfg.WAL.Enabled = true
						cfg.Replication.Role = config.RoleMaster
						cfg.Replication.ListenAddress = address.address
						if target == "promotion" {
							cfg.Replication.Role = config.RoleStandby
							cfg.Replication.MasterAddress = "127.0.0.1:3224"
						}
					}
					err := cfg.Validate()
					if address.loopback || allow {
						require.NoError(t, err, "%s allow=%v", target, allow)
					} else {
						require.ErrorContains(t, err, "network.allow_remote", "%s", target)
					}
				}
			}
		})
	}
}

func TestInvalidListenAddressFailsEvenWithRemoteOptIn(t *testing.T) {
	t.Parallel()
	for _, address := range []string{"127.0.0.1", "::1:3223", "127.0.0.1:", "127.0.0.1:-1", "127.0.0.1:65536"} {
		cfg := config.DefaultServerConfig()
		cfg.Network.Address = address
		cfg.Network.AllowRemote = true
		require.Error(t, cfg.Validate(), address)
	}
}

func TestReplicationBoundsMustBePositive(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"max_connections", "handshake_timeout", "idle_timeout"} {
		for _, role := range []string{config.RoleMaster, config.RoleStandby} {
			cfg := config.DefaultServerConfig()
			cfg.WAL.Enabled = true
			cfg.Replication.Role = role
			cfg.Replication.ListenAddress = "127.0.0.1:3224"
			cfg.Replication.MasterAddress = "127.0.0.1:3225"
			switch field {
			case "max_connections":
				cfg.Replication.MaxConnections = 0
			case "handshake_timeout":
				cfg.Replication.HandshakeTimeout = -1
			case "idle_timeout":
				cfg.Replication.IdleTimeout = 0
			}
			require.ErrorContains(t, cfg.Validate(), field)
		}
	}
}

func TestStandbyTimeoutExceedsHeartbeatInterval(t *testing.T) {
	t.Parallel()
	for _, timeout := range []time.Duration{time.Millisecond, time.Second - 1, time.Second, time.Second + 1, time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			t.Parallel()
			cfg := config.DefaultServerConfig()
			cfg.WAL.Enabled = true
			cfg.Replication.Role = config.RoleStandby
			cfg.Replication.MasterAddress = "127.0.0.1:3224"
			cfg.Replication.IdleTimeout = timeout
			err := cfg.Validate()
			if timeout <= time.Second {
				require.ErrorContains(t, err, "replication idle_timeout must exceed 1s for standby")
			} else {
				require.NoError(t, err)
			}
			// A master's timeout bounds writes and adjusts its own heartbeat cadence.
			cfg.Replication.Role = config.RoleMaster
			cfg.Replication.ListenAddress = "127.0.0.1:3224"
			require.NoError(t, cfg.Validate())
		})
	}
}

func TestRemoteEnvironmentOptIn(t *testing.T) { //nolint:paralleltest // environment overrides are process-wide
	path := filepath.Join(t.TempDir(), "server.yaml")
	require.NoError(t, os.WriteFile(path, []byte("network:\n  address: 0.0.0.0:3223\n  allow_remote: true\n"), 0o600))
	t.Setenv("DB_ALLOW_REMOTE", "false")
	_, err := config.LoadServerConfig(path)
	require.ErrorContains(t, err, "network.allow_remote")
	t.Setenv("DB_ALLOW_REMOTE", "true")
	cfg, err := config.LoadServerConfig(path)
	require.NoError(t, err)
	require.True(t, cfg.Network.AllowRemote)
	t.Setenv("DB_ALLOW_REMOTE", "maybe")
	_, err = config.LoadServerConfig(path)
	require.ErrorContains(t, err, "DB_ALLOW_REMOTE")
}
