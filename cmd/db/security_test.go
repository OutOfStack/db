package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestClientSecurityContract(t *testing.T) {
	t.Parallel()
	const sentinel = "secret-client-payload"
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store := storage.New(engine.New(), storage.WithReadOnly(true))
	admin := &replicationAdmin{store: store, logger: logger, role: config.RoleStandby}
	c := compute.New(parser.New(), store, logger, compute.WithAdmin(admin))
	srv, err := network.NewTCPServer("127.0.0.1:0", logger, network.WithServerMaxConnections(1))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(requestHandler(c)) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, srv.Shutdown(ctx))
		require.NoError(t, <-done)
		require.NotContains(t, logs.String(), sentinel)
		require.NotContains(t, logs.String(), "args=")
		require.Contains(t, logs.String(), "Connection limit reached")
	})
	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", srv.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	reader := bufio.NewReader(conn)
	require.NoError(t, protocol.WriteCommand(conn, "PROMOTE", nil))
	reply, err := protocol.ReadReply(reader, 1024)
	require.NoError(t, err)
	require.Equal(t, protocol.ReplyError, reply.Kind)
	require.Contains(t, reply.Value, "allow_remote_promote")
	require.True(t, store.ReadOnly())

	rejected, err := dialer.DialContext(t.Context(), "tcp", srv.Addr().String())
	require.NoError(t, err)
	defer func() { _ = rejected.Close() }()
	require.NoError(t, rejected.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = rejected.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)

	// The original client can still send requests after the excess connection is refused.
	require.NoError(t, protocol.WriteCommand(conn, "SET", []string{sentinel, sentinel, sentinel}))
	reply, err = protocol.ReadReply(reader, 1024)
	require.NoError(t, err)
	require.Equal(t, protocol.ReplyError, reply.Kind)
	require.Contains(t, reply.Value, "readonly")
	_, err = io.WriteString(conn, "*"+sentinel+"\r\n")
	require.NoError(t, err)
	reply, err = protocol.ReadReply(reader, 1024)
	require.NoError(t, err)
	require.Equal(t, protocol.ReplyError, reply.Kind)
}
