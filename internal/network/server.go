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

	// errorDrainTimeout bounds draining of unread request bytes before closing a connection after a protocol error, so the
	// error reply is not lost to a TCP reset caused by closing with pending input
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
	mu                  sync.Mutex
	connections         map[net.Conn]bool
	draining            bool
	cancelHandlers      context.CancelFunc
	serveDone           chan struct{}

	idleTimeout    time.Duration
	maxMessageSize int
}

// NewTCPServer creates a new Server instance with the given configuration and logger. It initializes the server with
// default values and sets up connection management.
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
		connections:         make(map[net.Conn]bool),
		serveDone:           make(chan struct{}),
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

// Serve accepts connections until Shutdown closes the listener.
func (s *TCPServer) Serve(handler RequestHandler) error {
	handlerCtx, cancelHandlers := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancelHandlers = cancelHandlers
	s.mu.Unlock()
	defer func() {
		if !s.isDraining() {
			cancelHandlers()
		}
	}()
	defer close(s.serveDone)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.logger.Error("Failed to accept connection", "error", err)
			continue
		}

		select {
		case s.connectionSemaphore <- struct{}{}:
			if !s.trackConnection(conn) {
				<-s.connectionSemaphore
				_ = conn.Close()
				continue
			}
			go s.handleConnection(handlerCtx, conn, handler)
		default:
			s.logger.Warn("Connection limit reached, rejecting new connection", "client", conn.RemoteAddr())
			if err = conn.Close(); err != nil {
				s.logger.Error("Failed to close rejected connection", "error", err)
			}
		}
	}
}

// Shutdown must be called while Serve is running. It stops accepting, closes idle connections, and gives active handlers
// until ctx expires before cancelling their contexts and closing their sockets. The deadline bounds that grace period;
// Shutdown still waits for every handler to return so persistence can be closed safely.
func (s *TCPServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	idle := make([]net.Conn, 0, len(s.connections))
	for conn, active := range s.connections {
		if !active {
			idle = append(idle, conn)
		}
	}
	s.mu.Unlock()

	err := s.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	for _, conn := range idle {
		if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	<-s.serveDone

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		s.cancelActiveHandlers()
		return err
	case <-ctx.Done():
		s.logger.Warn("Shutdown grace period expired; cancelling active handlers", "error", ctx.Err())
		s.cancelActiveHandlers()
		s.mu.Lock()
		active := make([]net.Conn, 0, len(s.connections))
		for conn := range s.connections {
			active = append(active, conn)
		}
		s.mu.Unlock()
		for _, conn := range active {
			if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
				err = errors.Join(err, closeErr)
			}
		}
		<-done
		return err
	}
}

func (s *TCPServer) cancelActiveHandlers() {
	s.mu.Lock()
	cancel := s.cancelHandlers
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *TCPServer) trackConnection(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.connections[conn] = false
	s.wg.Add(1)
	return true
}

func (s *TCPServer) beginCommand(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		// Shutdown won the race after this command was decoded, so it is not dispatched. The closed connection can make
		// the client report ErrOutcomeUnknown even though the command did not execute.
		return false
	}
	s.connections[conn] = true
	return true
}

func (s *TCPServer) finishCommand(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[conn] = false
	return s.draining
}

func (s *TCPServer) handleConnection(handlerCtx context.Context, conn net.Conn, handler RequestHandler) {
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Error("Failed to close connection", "error", err)
		}
		// release connection slot
		<-s.connectionSemaphore
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	s.logger.Info("Client connected", "address", conn.RemoteAddr())

	reader := bufio.NewReader(conn)

	for {
		cmd, args, ok := s.readCommand(conn, reader)
		if !ok {
			return
		}

		// Process the request. Dispatch happens only after ReadCommand has decoded the frame whole, so a truncated request
		// returns above without ever reaching the handler. Clients rely on that to tell a failed send apart from a lost
		// reply: a command they could not finish writing provably did not run.
		if !s.beginCommand(conn) {
			return
		}
		response := handler(handlerCtx, cmd, args)
		if err := s.writeReply(conn, response); err != nil {
			s.logger.Error("Failed to send response", "error", err)
			return
		}

		if s.finishCommand(conn) {
			return
		}
	}
}

func (s *TCPServer) readCommand(conn net.Conn, reader *bufio.Reader) (string, []string, bool) {
	if err := conn.SetReadDeadline(time.Now().Add(s.idleTimeout)); err != nil {
		s.logger.Error("Failed to set read deadline", "error", err)
		return "", nil, false
	}

	cmd, args, err := protocol.ReadCommand(reader, s.maxMessageSize)
	if err == nil {
		return cmd, args, true
	}
	if s.isDraining() || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "", nil, false
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		s.logger.Info("Closing idle connection", "address", conn.RemoteAddr())
		return "", nil, false
	}
	s.logger.Error("Error reading command from connection", "error", err)
	if writeErr := s.writeReply(conn, protocol.Error(err.Error())); writeErr != nil {
		s.logger.Error("Failed to send protocol error", "error", writeErr)
		return "", nil, false
	}
	// Closing with unread input can reset the connection before the queued error reply reaches the client.
	if drainErr := conn.SetReadDeadline(time.Now().Add(errorDrainTimeout)); drainErr == nil {
		_, _ = io.Copy(io.Discard, reader)
	}
	return "", nil, false
}

func (s *TCPServer) isDraining() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.draining
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
