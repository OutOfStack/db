//go:build !windows

package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

const (
	shutdownHelperAddress = "DB_TEST_SHUTDOWN_ADDRESS"
	shutdownHelperDataDir = "DB_TEST_SHUTDOWN_DATA_DIR"
)

func TestSIGTERMPreservesAcknowledgedWrite(t *testing.T) {
	dataDir := t.TempDir()
	address := freeAddr(t)
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestShutdownHelperProcess$")
	cmd.Env = append(os.Environ(), shutdownHelperAddress+"="+address, shutdownHelperDataDir+"="+dataDir)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())

	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	stopped := false
	defer func() {
		if !stopped {
			_ = cmd.Process.Kill()
			<-waitDone
		}
	}()

	client := network.NewTCPClient(address, network.WithClientIdleTimeout(100*time.Millisecond))
	defer func() { _ = client.Close() }()
	waitFor(t, "server process to accept commands", func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		_, err := client.Send(ctx, "GET", []string{"health", "ready"})
		return err == nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	reply, err := client.Send(ctx, "SET", []string{"users", "name", "vlad"})
	cancel()
	require.NoError(t, err)
	require.Equal(t, protocol.SimpleString("OK"), reply)
	require.NoError(t, client.Close())
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	select {
	case err = <-waitDone:
		stopped = true
		require.NoError(t, err, output.String())
	case <-time.After(5 * time.Second):
		t.Fatalf("server did not stop after SIGTERM: %s", output.String())
	}

	cfg := shutdownTestConfig(address, dataDir)
	dbEngine, writer, _, err := recoverPersistence(cfg, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Close()) }()
	value, err := dbEngine.Get(t.Context(), "users", "name")
	require.NoError(t, err)
	require.Equal(t, "vlad", stored(value))
}

func TestShutdownHelperProcess(t *testing.T) {
	address := os.Getenv(shutdownHelperAddress)
	if address == "" {
		return
	}
	cfg := shutdownTestConfig(address, os.Getenv(shutdownHelperDataDir))
	require.NoError(t, run(cfg, slog.New(slog.DiscardHandler), false))
}

func shutdownTestConfig(address, dataDir string) *config.ServerConfig {
	cfg := config.DefaultServerConfig()
	cfg.Network.Address = address
	cfg.Network.ShutdownTimeout = time.Second
	cfg.WAL.Enabled = true
	cfg.WAL.DataDir = dataDir
	cfg.WAL.Sync = wal.SyncAlways
	cfg.WAL.SegmentSizeMB = 1
	return cfg
}
