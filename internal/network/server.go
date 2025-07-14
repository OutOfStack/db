package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

// RequestHandler is a function that handles a client request
type RequestHandler func(context.Context, []byte) []byte

// TCPServer represents a TCP server that handles multiple client connections
type TCPServer struct {
	logger              *slog.Logger
	listener            net.Listener
	wg                  sync.WaitGroup
	connectionSemaphore chan struct{}

	idleTimeout time.Duration
	bufferSize  int
}

// NewTCPServer creates a new Server instance with the given configuration and logger.
// It initializes the server with default values and sets up connection management.
func NewTCPServer(address string, logger *slog.Logger, options ...TCPServerOption) (*TCPServer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %w", err)
	}

	server := &TCPServer{
		listener:            listener,
		logger:              logger,
		connectionSemaphore: make(chan struct{}, 100),
	}

	for _, option := range options {
		option(server)
	}

	return server, nil
}

// Start begins accepting connections
func (s *TCPServer) Start(ctx context.Context, handler RequestHandler) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				s.logger.Error("Failed to accept connection", "error", err)
				continue
			}

			// try to acquire connection slot
			select {
			case s.connectionSemaphore <- struct{}{}:
				// connection slot acquired, handle the connection
				s.wg.Add(1)
				go s.handleConnection(ctx, conn, handler)
			default:
				// connection limit reached, reject the connection
				s.logger.Warn("Connection limit reached, rejecting new connection", "client", conn.RemoteAddr())
				if err = conn.Close(); err != nil {
					s.logger.Error("Failed to close rejected connection", "error", err)
				}
			}
		}
	}()

	<-ctx.Done()

	if err := s.listener.Close(); err != nil {
		s.logger.Error("Failed to stop server", "error", err)
	}

	s.wg.Wait()
}

func (s *TCPServer) handleConnection(ctx context.Context, conn net.Conn, handler RequestHandler) {
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Error("Failed to close connection", "error", err)
		}
		// release connection slot
		<-s.connectionSemaphore
		s.wg.Done()
	}()

	s.logger.Info("Client connected", "address", conn.RemoteAddr())

	buf := make([]byte, s.bufferSize)

	for {
		// set the read deadline
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			s.logger.Error("Failed to set read deadline", "error", err)
			return
		}

		// read data from the connection
		n, err := conn.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			s.logger.Error("Error reading from connection", "error", err)
			return
		}

		if n > 0 {
			if n == s.bufferSize {
				s.logger.Warn("Request too large")
				break
			}

			// process the request
			response := handler(ctx, buf[:n])

			// set the write deadline
			if err = conn.SetWriteDeadline(time.Now().Add(s.idleTimeout)); err != nil {
				s.logger.Error("Failed to set write deadline", "error", err)
				return
			}
			// send the response
			if _, err = conn.Write(response); err != nil {
				s.logger.Error("Failed to send response", "error", err)
				return
			}
		}

		// check for context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
