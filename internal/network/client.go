package network

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
)

// ErrOutcomeUnknown reports that a command reached the server in full but no reply could be read, so whether it took
// effect is unknowable from the client side. Re-issuing a read or an idempotent SET is harmless; INCR, APPEND and HSET
// have to be checked against the server before being repeated.
var ErrOutcomeUnknown = errors.New("command outcome unknown: sent without a reply")

// TCPClient is a client connection to a single server.
//
// It is safe for concurrent use. Commands are serialized, so one connection carries one in-flight command at a time.
//
// Send reports delivery in three ways, and callers that mutate state have to distinguish them:
//
//   - nil error: the command executed and the reply is valid.
//   - ErrOutcomeUnknown: the command was sent, but its outcome cannot be determined.
//   - any other error: the command did not execute, or it was read-only and re-issuing it is harmless.
type TCPClient struct {
	// sendGate serializes commands, holding one token for the command in flight. The protocol is strictly
	// request/response: concurrent writers would interleave frames, and concurrent readers would steal each other's
	// replies. It is a channel rather than a mutex so that waiting for a turn stays cancellable — a caller's deadline has
	// to cover queueing too, not just its own I/O.
	sendGate chan struct{}

	// closeCh is closed by Close. It releases callers queued on sendGate, who would otherwise wait for a command that
	// Close has already interrupted.
	closeCh chan struct{}

	// mu guards the connection state. Close takes only this lock, never sendGate, so it can drop the socket and thereby
	// unblock a command parked in a read.
	mu     sync.Mutex
	conn   net.Conn
	reader *bufio.Reader
	closed bool
	// dialCancel aborts a dial in progress. Only the command holding sendGate can dial, so one is enough.
	dialCancel context.CancelFunc

	address        string
	idleTimeout    time.Duration
	maxMessageSize int
}

// NewTCPClient creates a client for address. It does not connect: the socket is opened by the first command, under that
// command's context and deadline, and reopened after any failure.
func NewTCPClient(address string, options ...TCPClientOption) *TCPClient {
	client := &TCPClient{
		sendGate:       make(chan struct{}, 1),
		closeCh:        make(chan struct{}),
		address:        address,
		maxMessageSize: defaultMaxMessageSize,
		idleTimeout:    defaultTimeout,
	}

	for _, option := range options {
		option(client)
	}

	return client
}

// Send sends a command and returns the server's reply. See TCPClient for how failures are reported.
func (tc *TCPClient) Send(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	if err := tc.enter(ctx); err != nil {
		return protocol.Reply{}, err
	}
	defer func() { <-tc.sendGate }()

	// one budget covers dial, write and read; WithDeadline keeps whichever of the caller's deadline and the idle timeout
	// falls first. It starts here rather than at entry so that time spent queueing is not also charged to the idle
	// timeout, while the caller's own deadline still bounds the whole call.
	sendCtx, cancel := context.WithDeadline(ctx, time.Now().Add(tc.idleTimeout))
	defer cancel()

	mutation := parser.IsMutation(cmd)

	resp, retry, err := tc.attempt(ctx, sendCtx, cmd, args, mutation)
	if err != nil && retry {
		resp, _, err = tc.attempt(ctx, sendCtx, cmd, args, mutation)
	}
	return resp, err
}

// enter waits for this command's turn on the connection. Queueing behind another command is part of the call, so it
// ends when the caller's context does — otherwise a request with a 10ms deadline could sit behind one still inside its
// minute-long idle timeout.
func (tc *TCPClient) enter(ctx context.Context) error {
	// prefer making progress: a free gate wins over a context that has only just expired
	select {
	case tc.sendGate <- struct{}{}:
		return nil
	default:
	}

	select {
	case tc.sendGate <- struct{}{}:
		return nil
	case <-tc.closeCh:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// attempt runs one round trip. It reports whether re-sending the command on a fresh connection is safe, which is only
// ever true when the command provably did not execute.
func (tc *TCPClient) attempt(ctx, sendCtx context.Context, cmd string, args []string, mutation bool) (protocol.Reply, bool, error) {
	conn, reader, err := tc.acquire(sendCtx)
	if err != nil {
		// nothing was written, so the command did not execute; a retired client is the one case not worth another try
		return protocol.Reply{}, !errors.Is(err, net.ErrClosed), err
	}

	deadline, _ := sendCtx.Deadline()
	if err = conn.SetDeadline(deadline); err != nil {
		tc.drop(conn)
		return protocol.Reply{}, true, fmt.Errorf("failed to set deadline: %w", err)
	}

	// A deadline is the only lever a net.Conn offers for interrupting a blocked read, so cancellation expires it. This is
	// armed after SetDeadline above, which would otherwise overwrite an expiry that arrived first.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer func() {
		// stop reports that the callback was descheduled, not that it finished. A callback still in flight would expire
		// the deadline of whichever command picks this connection up next, so retire the connection instead.
		if !stop() {
			tc.drop(conn)
		}
	}()

	if err = protocol.WriteCommand(conn, cmd, args); err != nil {
		tc.drop(conn)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return protocol.Reply{}, false, ctxErr
		}
		// A write error means part of the frame never left this machine, and the server dispatches a command only once it
		// has decoded the frame whole (see handleConnection), so the command cannot have executed. Dropping the connection
		// above is what makes that hold: it guarantees the orphaned prefix can never be completed.
		return protocol.Reply{}, true, fmt.Errorf("failed to send data: %w", err)
	}

	resp, err := protocol.ReadReply(reader, tc.maxMessageSize)
	if err == nil {
		return resp, false, nil
	}

	// The request is on the wire and the reply is not. The connection cannot be reused either way: unread reply bytes
	// would be mistaken for the next command's reply.
	tc.drop(conn)

	if mutation {
		cause := err
		if ctxErr := ctx.Err(); ctxErr != nil {
			cause = ctxErr
		}
		return protocol.Reply{}, false, fmt.Errorf("%w: %w", ErrOutcomeUnknown, cause)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return protocol.Reply{}, false, ctxErr
	}
	// A read-only command has no side effects, so a broken connection is worth one more try. A decode error is not: the
	// server answered, and asking again would only produce the same undecodable reply.
	return protocol.Reply{}, isConnectionError(err), fmt.Errorf("failed to read response: %w", err)
}

// acquire returns a connection to send on, dialing when the client holds none and when the one it holds is no longer
// healthy.
func (tc *TCPClient) acquire(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	tc.mu.Lock()
	closed, conn, reader := tc.closed, tc.conn, tc.reader
	tc.mu.Unlock()

	if closed {
		return nil, nil, net.ErrClosed
	}
	if conn != nil {
		if alive(conn, reader) {
			return conn, reader, nil
		}
		tc.drop(conn)
	}
	return tc.dial(ctx)
}

// dial opens a connection and installs it as the current one. It dials outside tc.mu so that Close never waits behind a
// connection attempt to an unresponsive host, and publishes its cancel func so that Close can abort one: an
// unreachable address would otherwise keep the dial alive for the whole deadline after Close returned.
func (tc *TCPClient) dial(ctx context.Context) (net.Conn, *bufio.Reader, error) {
	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tc.mu.Lock()
	if tc.closed {
		tc.mu.Unlock()
		return nil, nil, net.ErrClosed
	}
	tc.dialCancel = cancel
	tc.mu.Unlock()

	var dialer net.Dialer
	conn, err := dialer.DialContext(dialCtx, "tcp", tc.address)

	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.dialCancel = nil

	if err != nil {
		if tc.closed {
			return nil, nil, net.ErrClosed
		}
		return nil, nil, fmt.Errorf("failed to connect to %s: %w", tc.address, err)
	}
	if tc.closed {
		// Close ran while this connection was being established
		_ = conn.Close()
		return nil, nil, net.ErrClosed
	}

	tc.conn, tc.reader = conn, bufio.NewReader(conn)
	return tc.conn, tc.reader, nil
}

// alive reports whether a connection left over from an earlier command is still usable. The protocol is strictly
// request/response, so an idle connection has nothing to read: a timeout means healthy, readable bytes mean the stream
// is out of step, and anything else means the peer is gone.
//
// This check is what keeps a server's own idle close from looking like a lost reply. It moves that failure back before
// the send, where re-sending a mutation is safe. A peer that disappears after this point is the genuine ambiguity that
// ErrOutcomeUnknown exists to report.
func alive(conn net.Conn, reader *bufio.Reader) bool {
	if reader.Buffered() > 0 {
		return false
	}
	if err := conn.SetReadDeadline(time.Now()); err != nil {
		return false
	}

	// Peek leaves the stream untouched, and bufio clears the deadline error it stores, so the next real read is unaffected
	_, err := reader.Peek(1)
	netErr, ok := errors.AsType[net.Error](err)
	return ok && netErr.Timeout()
}

// drop retires conn, unless a later command has already replaced it.
func (tc *TCPClient) drop(conn net.Conn) {
	tc.mu.Lock()
	owned := tc.conn == conn
	if owned {
		tc.conn, tc.reader = nil, nil
	}
	tc.mu.Unlock()

	if owned {
		_ = conn.Close()
	}
}

// isConnectionError reports whether err means the connection broke, as opposed to the server sending a reply the client
// could not decode. Only read-only commands act on it, by re-sending on a fresh connection.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// EOF and its mid-frame form both mean the peer went away while the reply was outstanding
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return true
	}

	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		switch opErr.Op {
		case "read", "write", "dial":
			return true
		}
	}

	return false
}

// Close closes the connection and retires the client: later commands fail with net.ErrClosed rather than reconnecting.
// It is safe to call more than once and from any goroutine, including while a command is in flight — closing the socket
// unblocks a command parked in a read.
func (tc *TCPClient) Close() error {
	tc.mu.Lock()
	if tc.closed {
		tc.mu.Unlock()
		return nil
	}
	tc.closed = true
	conn, dialCancel := tc.conn, tc.dialCancel
	tc.conn, tc.reader, tc.dialCancel = nil, nil, nil
	close(tc.closeCh) // release anyone queued for a turn
	tc.mu.Unlock()

	// a command is either dialing or using a socket, never both
	if dialCancel != nil {
		dialCancel()
	}
	if conn == nil {
		return nil
	}
	return conn.Close()
}
