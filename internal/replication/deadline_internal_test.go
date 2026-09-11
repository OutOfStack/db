package replication

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestMasterDropsBlockedStream(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: wal.SyncAlways, SegmentSize: 1 << 20}, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	_, err = writer.Append(t.Context(), wal.CommandSet, []string{"t", "k", strings.Repeat("v", 8192)})
	require.NoError(t, err)
	m := &Master{writer: writer, dir: dir, logger: slog.New(slog.DiscardHandler),
		heartbeatInterval: time.Second, limits: defaultPeerLimits()}
	m.limits.idleTimeout = 100 * time.Millisecond
	server, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.handleConn(t.Context(), server)
	}()
	require.NoError(t, peer.SetDeadline(time.Now().Add(3*time.Second)))
	require.NoError(t, writeHandshake(peer, 0))
	// Consume only the start of a record, then stop reading while the master still has payload to send.
	_, err = io.ReadFull(peer, make([]byte, 16))
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked replication write did not time out")
	}
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestStreamingReadDeadlineRenewsAfterProgress(t *testing.T) {
	t.Parallel()
	server, peer := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = peer.Close() }()
	conn := deadlineConn{Conn: server, timeout: 200 * time.Millisecond}
	done := make(chan error, 1)
	go func() {
		for range 8 {
			time.Sleep(50 * time.Millisecond)
			if _, err := peer.Write([]byte{1}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	_, err := io.ReadFull(conn, make([]byte, 8))
	require.NoError(t, err)
	require.NoError(t, <-done)
	_, err = conn.Read(make([]byte, 1))
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout())
}

func TestHandshakeDeadlineDoesNotRenewForTrickledBytes(t *testing.T) {
	t.Parallel()
	m := &Master{logger: slog.New(slog.DiscardHandler), limits: defaultPeerLimits()}
	m.limits.handshakeTimeout = 150 * time.Millisecond
	server, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.handleConn(t.Context(), server)
	}()
	require.NoError(t, peer.SetDeadline(time.Now().Add(2*time.Second)))
	_, err := io.WriteString(peer, "*2\r\n$9\r\n")
	require.NoError(t, err)
	written := 0
	for _, b := range []byte("REPLICATE") {
		if _, err = peer.Write([]byte{b}); err != nil {
			break
		}
		written++
		time.Sleep(40 * time.Millisecond)
	}
	require.Less(t, written, len("REPLICATE"), "trickled bytes extended the whole-handshake deadline")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handshake handler did not exit")
	}
}
