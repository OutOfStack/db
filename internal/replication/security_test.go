package replication_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/replication"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

func dialPeer(t *testing.T, address string) net.Conn {
	t.Helper()
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	return conn
}

func requirePeerClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err)
	if netErr, ok := errors.AsType[net.Error](err); ok {
		require.False(t, netErr.Timeout(), "peer was not closed before the test deadline: %v", err)
	}
}

func TestMasterHandshakeDeadline(t *testing.T) {
	t.Parallel()
	for _, partial := range []string{"", "*2\r\n$9\r\nREPLICATE\r\n$1\r\n"} {
		t.Run(partial, func(t *testing.T) {
			t.Parallel()
			n := newNode(t, t.TempDir())
			m, err := replication.NewMaster("127.0.0.1:0", n.writer, n.dir, nil,
				replication.WithMaxConnections(1), replication.WithHandshakeTimeout(100*time.Millisecond))
			require.NoError(t, err)
			go m.Serve(t.Context())
			t.Cleanup(func() { require.NoError(t, m.Close()) })
			conn := dialPeer(t, m.Addr().String())
			if partial != "" {
				_, err = io.WriteString(conn, partial)
				require.NoError(t, err)
			}
			requirePeerClosed(t, conn)
			// A completed handshake after the timeout proves the stalled connection released its slot.
			next := dialPeer(t, m.Addr().String())
			require.NoError(t, protocol.WriteCommand(next, "REPLICATE", []string{"0"}))
			heartbeat := make([]byte, 9)
			_, err = io.ReadFull(next, heartbeat)
			require.NoError(t, err)
			require.Equal(t, byte('H'), heartbeat[0])
		})
	}
}

func TestMasterConnectionCapPreservesExistingStream(t *testing.T) {
	t.Parallel()
	n := newNode(t, t.TempDir())
	m, err := replication.NewMaster("127.0.0.1:0", n.writer, n.dir, nil,
		replication.WithMaxConnections(1), replication.WithIdleTimeout(200*time.Millisecond))
	require.NoError(t, err)
	go m.Serve(t.Context())
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	first := dialPeer(t, m.Addr().String())
	require.NoError(t, protocol.WriteCommand(first, "REPLICATE", []string{"0"}))
	reader := bufio.NewReader(first)
	heartbeat := make([]byte, 9)
	_, err = io.ReadFull(reader, heartbeat)
	require.NoError(t, err)
	require.Equal(t, byte('H'), heartbeat[0])
	rejected := dialPeer(t, m.Addr().String())
	requirePeerClosed(t, rejected)
	set(t, n, "t", "k", "still streaming")
	for {
		kind, readErr := reader.ReadByte()
		require.NoError(t, readErr)
		if kind == 'H' {
			_, readErr = io.ReadFull(reader, heartbeat[:8])
			require.NoError(t, readErr)
			continue
		}
		require.Equal(t, byte('R'), kind)
		record, recordErr := wal.ReadRecord(reader)
		require.NoError(t, recordErr)
		require.Equal(t, uint64(1), record.LSN)
		break
	}
}

func TestStandbyDropsStalledMaster(t *testing.T) {
	t.Parallel()
	for _, partial := range []string{"", "H\x00", "R\x00", "S\x00",
		"S\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x01\x00DBSNP"} {
		t.Run(partial, func(t *testing.T) {
			t.Parallel()
			lc := net.ListenConfig{}
			listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			n := newNode(t, t.TempDir())
			s := replication.NewStandby(listener.Addr().String(), n.store, n.dir, 0, time.Hour, nil,
				replication.WithIdleTimeout(100*time.Millisecond))
			s.Start(t.Context())
			t.Cleanup(s.Stop)
			conn, err := listener.Accept()
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
			cmd, _, err := protocol.ReadCommand(bufio.NewReader(conn), 128)
			require.NoError(t, err)
			require.Equal(t, "REPLICATE", cmd)
			if partial != "" {
				_, err = io.WriteString(conn, partial)
				require.NoError(t, err)
			}
			requirePeerClosed(t, conn)
			waitFor(t, "standby disconnected", func() bool { return !s.Connected() })
			require.Zero(t, s.AppliedLSN())
			require.Zero(t, n.writer.LastLSN())
		})
	}
}

func TestMasterCloseInterruptsHandshakeWithoutContextCancellation(t *testing.T) {
	t.Parallel()
	n := newNode(t, t.TempDir())
	m, err := replication.NewMaster("127.0.0.1:0", n.writer, n.dir, nil,
		replication.WithHandshakeTimeout(time.Hour))
	require.NoError(t, err)
	go m.Serve(context.Background())
	conn := dialPeer(t, m.Addr().String())
	require.NoError(t, m.Close())
	requirePeerClosed(t, conn)
}
