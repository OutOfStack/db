package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/protocol"
)

const (
	// defaultMaxMessageSize mirrors the 4KB config default (max_message_size)
	defaultMaxMessageSize = 4096
	defaultTimeout        = 1 * time.Minute

	// errorDrainTimeout bounds draining of unread request bytes before closing
	// a connection after a protocol error, so the error reply is not lost to a
	// TCP reset caused by closing with pending input
	errorDrainTimeout = 100 * time.Millisecond
)

// RequestHandler is a function that handles a decoded client command.
type RequestHandler func(context.Context, string, []string) protocol.Reply

// TCPServer represents a TCP server that handles multiple client connections
type TCPServer struct {
	logger              *slog.Logger
	listener            net.Listener
	wg                  sync.WaitGroup
	connectionSemaphore chan struct{}

	idleTimeout    time.Duration
	maxMessageSize int
}

// NewTCPServer creates a new Server instance with the given configuration and logger.
// It initializes the server with default values and sets up connection management.
func NewTCPServer(address string, logger *slog.Logger, options ...TCPServerOption) (*TCPServer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to start server: %w", err)
	}

	server := &TCPServer{
		listener:            listener,
		logger:              logger,
		connectionSemaphore: make(chan struct{}, 100),
		maxMessageSize:      defaultMaxMessageSize,
		idleTimeout:         defaultTimeout,
	}

	for _, option := range options {
		option(server)
	}

	return server, nil
}

// Addr returns the address the server is listening on
func (s *TCPServer) Addr() net.Addr {
	return s.listener.Addr()
}

// Start begins accepting connections
func (s *TCPServer) Start(ctx context.Context, handler RequestHandler) {
	s.wg.Go(func() {
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
	})

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

	reader := bufio.NewReader(conn)

	for {
		// set the read deadline
		if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			s.logger.Error("Failed to set read deadline", "error", err)
			return
		}

		// read one full RESP command from the connection
		cmd, args, err := protocol.ReadCommand(reader, s.maxMessageSize)
		if err != nil {
			// client disconnected (possibly mid-frame): close quietly
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}
			// idle timeout is a normal disconnect, not an error
			if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
				s.logger.Info("Closing idle connection", "address", conn.RemoteAddr())
				return
			}
			s.logger.Error("Error reading command from connection", "error", err)
			if wErr := s.writeReply(conn, protocol.Error(err.Error())); wErr != nil {
				s.logger.Error("Failed to send protocol error", "error", wErr)
				return
			}
			// drain pending request bytes briefly: closing with unread input
			// can trigger a TCP reset that discards the queued error reply
			if dErr := conn.SetReadDeadline(time.Now().Add(errorDrainTimeout)); dErr == nil {
				_, _ = io.Copy(io.Discard, reader)
			}
			return
		}

		// process request
		response := handler(ctx, cmd, args)
		if err = s.writeReply(conn, response); err != nil {
			s.logger.Error("Failed to send response", "error", err)
			return
		}

		// check for context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (s *TCPServer) writeReply(conn net.Conn, reply protocol.Reply) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.idleTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}
	if err := protocol.WriteReply(conn, reply); err != nil {
		return fmt.Errorf("failed to write reply: %w", err)
	}
	return nil
}
