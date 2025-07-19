package network

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	network_mocks "github.com/OutOfStack/db/internal/network/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"log/slog"
)

func TestTCPClientSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := network_mocks.NewMockConn(ctrl)
	client := &TCPClient{
		conn:        mockConn,
		address:     "addr",
		idleTimeout: time.Second,
		bufferSize:  8,
	}

	data := []byte("ping")
	respData := []byte("pong")

	gomock.InOrder(
		mockConn.EXPECT().SetWriteDeadline(gomock.Any()).Return(nil),
		mockConn.EXPECT().Write(data).Return(len(data), nil),
		mockConn.EXPECT().SetReadDeadline(gomock.Any()).Return(nil),
		mockConn.EXPECT().Read(gomock.Any()).DoAndReturn(func(b []byte) (int, error) {
			copy(b, respData)
			return len(respData), nil
		}),
	)

	resp, err := client.Send(data)
	require.NoError(t, err)
	require.Equal(t, respData, resp)
}

func TestTCPClientClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConn := network_mocks.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Return(nil)

	client := &TCPClient{conn: mockConn}
	require.NoError(t, client.Close())
}

func TestTCPServerStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	serverConn, clientConn := net.Pipe()

	listener := network_mocks.NewMockListener(ctrl)
	listener.EXPECT().Accept().Return(serverConn, nil).Times(1)
	listener.EXPECT().Accept().Return(nil, net.ErrClosed).AnyTimes()
	listener.EXPECT().Close().Return(nil).Times(1)
	listener.EXPECT().Addr().AnyTimes()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := &TCPServer{
		logger:              logger,
		listener:            listener,
		connectionSemaphore: make(chan struct{}, 1),
		bufferSize:          32,
		idleTimeout:         time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.Start(ctx, func(_ context.Context, b []byte) []byte { return bytes.ToUpper(b) })
		close(done)
	}()

	// send request
	_, err := clientConn.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 32)
	n, err := clientConn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, []byte("PING"), buf[:n])

	cancel()
	clientConn.Close()
	<-done
}

func TestOptions(t *testing.T) {
	c := &TCPClient{}
	WithClientIdleTimeout(5 * time.Second)(c)
	require.Equal(t, 5*time.Second, c.idleTimeout)
	WithClientBufferSize(128)(c)
	require.Equal(t, 128, c.bufferSize)

	s := &TCPServer{}
	WithServerIdleTimeout(3 * time.Second)(s)
	require.Equal(t, 3*time.Second, s.idleTimeout)
	WithServerBufferSize(64)(s)
	require.Equal(t, 64, s.bufferSize)
	WithServerMaxConnections(5)(s)
	require.Equal(t, cap(s.connectionSemaphore), 5)
}
