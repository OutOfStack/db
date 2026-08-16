package network_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/stretchr/testify/require"
)

func startTCPServer(t *testing.T, handler network.RequestHandler) (*network.TCPServer, <-chan error) {
	t.Helper()
	srv, err := network.NewTCPServer("127.0.0.1:0", slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(handler) }()
	return srv, done
}

func TestShutdownClosesIdleAndPartialConnections(t *testing.T) {
	t.Parallel()
	for _, partial := range []bool{false, true} {
		name := "idle"
		if partial {
			name = "partial frame"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv, serveDone := startTCPServer(t, func(context.Context, string, []string) protocol.Reply {
				return protocol.SimpleString("OK")
			})
			dialer := net.Dialer{}
			conn, err := dialer.DialContext(t.Context(), "tcp", srv.Addr().String())
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()
			if partial {
				_, err = io.WriteString(conn, "*2\r\n$3\r\nGET\r\n$1\r\n")
				require.NoError(t, err)
			}

			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			require.NoError(t, srv.Shutdown(ctx))
			require.NoError(t, <-serveDone)
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
			_, err = conn.Read(make([]byte, 1))
			require.Error(t, err)

			dialer.Timeout = 100 * time.Millisecond
			_, err = dialer.DialContext(t.Context(), "tcp", srv.Addr().String())
			require.Error(t, err)
		})
	}
}

func TestShutdownDrainsDecodedCommand(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	srv, serveDone := startTCPServer(t, func(context.Context, string, []string) protocol.Reply {
		close(started)
		<-release
		return protocol.SimpleString("OK")
	})
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(t.Context(), "tcp", srv.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, protocol.WriteCommand(conn, "PING", nil))
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- srv.Shutdown(ctx) }()
	select {
	case err = <-shutdownDone:
		t.Fatalf("Shutdown returned before active command completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	reply, err := protocol.ReadReply(bufio.NewReader(conn), 1024)
	require.NoError(t, err)
	require.Equal(t, protocol.SimpleString("OK"), reply)
	require.NoError(t, <-shutdownDone)
	require.NoError(t, <-serveDone)
}

func TestShutdownTimeoutCancelsStalledHandlerWithoutCloseError(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	cancelled := make(chan struct{})
	srv, serveDone := startTCPServer(t, func(ctx context.Context, _ string, _ []string) protocol.Reply {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return protocol.Error(ctx.Err().Error())
	})
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(t.Context(), "tcp", srv.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.NoError(t, protocol.WriteCommand(conn, "PING", nil))
	<-started

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = srv.Shutdown(ctx)
	require.NoError(t, err)
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("handler context was not cancelled")
	}
	require.NoError(t, <-serveDone)
	_, err = protocol.ReadReply(bufio.NewReader(conn), 1024)
	require.True(t, errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed), "ReadReply() error = %v", err)
}
