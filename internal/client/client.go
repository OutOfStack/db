package client

import (
	"fmt"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/pool"
)

// New creates a new client based on the configuration
// If pool is enabled, returns a pooled client; otherwise returns a single TCP client
func New(cfg *config.ClientConfig) (network.Client, error) {
	if cfg.Pool.Enabled {
		// Create pooled client
		poolClient, err := pool.NewClient(&cfg.Pool,
			network.WithClientIdleTimeout(cfg.Network.IdleTimeout),
			network.WithClientBufferSize(cfg.Network.MaxMessageSizeKB*1024),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create pool client: %w", err)
		}
		return poolClient, nil
	}

	// Create single TCP client
	tcpClient, err := network.NewTCPClient(cfg.Network.Address,
		network.WithClientIdleTimeout(cfg.Network.IdleTimeout),
		network.WithClientBufferSize(cfg.Network.MaxMessageSizeKB*1024),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP client: %w", err)
	}
	return tcpClient, nil
}
