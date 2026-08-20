package network_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/protocol"
)

// answer decides how a scripted server responds to the nth command it has received across all connections (n starts at
// 1). Returning a reply answers it; returning nil closes the connection unanswered, which is the lost-reply window the
// whole retry rule turns on; blocking holds the command open.
type answer func(n int32) *protocol.Reply

// scripted is a fake server driven by an answer function. Tests need it because network.RequestHandler can only produce
// a reply — it has no way to read a command and then drop the connection.
//
// It serves at most one command per connection and then closes, which is also what a server closing an idle connection
// looks like from the client side.
type scripted struct {
	addr     string
	received atomic.Int32
}

func startScripted(t *testing.T, fn answer) *scripted {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	s := &scripted{addr: ln.Addr().String()}
	go func() {
		for {
			conn, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			go s.serve(conn, fn)
		}
	}()
	return s
}

func (s *scripted) serve(conn net.Conn, fn answer) {
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	if _, _, err := protocol.ReadCommand(reader, 4096); err != nil {
		return
	}
	if reply := fn(s.received.Add(1)); reply != nil {
		_ = protocol.WriteReply(conn, *reply)
	}
}

func replyOK() *protocol.Reply {
	reply := protocol.SimpleString("OK")
	return &reply
}

// startServer runs a real in-process server with a custom handler on an ephemeral port.
func startServer(t *testing.T, handler network.RequestHandler) string {
	t.Helper()

	addr, _ := startServerAt(t, "127.0.0.1:0", handler)
	return addr
}

// startServerAt runs a server on a specific address and returns it with a stop function that blocks until the listener
// stops accepting, so a test can restart a server on the same address.
func startServerAt(t *testing.T, address string, handler network.RequestHandler) (addr string, stop func()) {
	t.Helper()

	srv, err := network.NewTCPServer(address, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("NewTCPServer(%s): %v", address, err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(handler) }()

	addr = srv.Addr().String()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if sErr := srv.Shutdown(ctx); sErr != nil {
				t.Errorf("Shutdown(%s): %v", addr, sErr)
			}
			if sErr := <-done; sErr != nil {
				t.Errorf("Serve(%s): %v", addr, sErr)
			}
		})
	}
	t.Cleanup(stop)
	return addr, stop
}

// startRude closes the first connection it accepts without reading a byte and serves the rest normally. A client
// writing a frame into that dead socket fails partway through it, which is the case the retry rule calls provably
// unexecuted.
func startRude(t *testing.T, received *atomic.Int32) string {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	var accepted atomic.Int32
	go func() {
		for {
			conn, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			if accepted.Add(1) == 1 {
				_ = conn.Close() // hang up before reading anything
				continue
			}
			go func() {
				defer func() { _ = conn.Close() }()
				if _, _, rErr := protocol.ReadCommand(bufio.NewReader(conn), 8<<20); rErr == nil {
					received.Add(1)
					_ = protocol.WriteReply(conn, protocol.SimpleString("OK"))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	for range 400 {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestSend_LostReplyIsOutcomeUnknown is the core of the delivery contract. A mutation whose reply is lost after the
// server already had the command is reported, never repeated: repeating it is how a single INCR became several.
func TestSend_LostReplyIsOutcomeUnknown(t *testing.T) {
	t.Parallel()

	srv := startScripted(t, func(int32) *protocol.Reply { return nil })
	c := network.NewTCPClient(srv.addr)
	t.Cleanup(func() { _ = c.Close() })

	_, err := c.Send(t.Context(), "INCR", []string{"stats", "hits"})
	if !errors.Is(err, network.ErrOutcomeUnknown) {
		t.Fatalf("Send() error = %v, want ErrOutcomeUnknown", err)
	}
	if got := srv.received.Load(); got != 1 {
		t.Errorf("server received %d commands, want exactly 1", got)
	}
}

// TestSend_ReadOnlyRetriesAfterLostReply is the other half of the rule. A read has no side effects, so a dropped reply
// has to stay invisible to the caller rather than becoming an error they must reason about.
func TestSend_ReadOnlyRetriesAfterLostReply(t *testing.T) {
	t.Parallel()

	srv := startScripted(t, func(n int32) *protocol.Reply {
		if n == 1 {
			return nil // first attempt: take the command, then vanish
		}
		return replyOK()
	})
	c := network.NewTCPClient(srv.addr)
	t.Cleanup(func() { _ = c.Close() })

	if _, err := c.Send(t.Context(), "GET", []string{"users", "name"}); err != nil {
		t.Fatalf("Send() error = %v, want the retry to succeed", err)
	}
	if got := srv.received.Load(); got != 2 {
		t.Errorf("server received %d commands, want 2 (the original and its retry)", got)
	}
}

// TestSend_StaleConnectionRetriesMutationOnce covers the case that makes the strict rule livable: a server closing a
// connection between commands. Noticing before sending keeps the mutation provably unsent, so it goes out exactly once
// on a fresh connection instead of being reported as unknown.
func TestSend_StaleConnectionRetriesMutationOnce(t *testing.T) {
	t.Parallel()

	srv := startScripted(t, func(int32) *protocol.Reply { return replyOK() })
	c := network.NewTCPClient(srv.addr)
	t.Cleanup(func() { _ = c.Close() })

	if _, err := c.Send(t.Context(), "SET", []string{"users", "name", "vlad"}); err != nil {
		t.Fatalf("first Send() error = %v", err)
	}

	// the scripted server closes after answering; give that close time to reach us, which is what an idle server's close
	// looks like in production
	time.Sleep(50 * time.Millisecond)

	if _, err := c.Send(t.Context(), "SET", []string{"users", "name", "vlad2"}); err != nil {
		t.Fatalf("second Send() error = %v, want a clean redial rather than an unknown outcome", err)
	}
	if got := srv.received.Load(); got != 2 {
		t.Errorf("server received %d commands, want 2 (one per Send, neither applied twice)", got)
	}
}

// TestClose_UnblocksInFlightAndIsTerminal pins the lock split: Close must not queue behind a command parked in a read,
// and a closed client must stay closed rather than quietly reconnecting.
func TestClose_UnblocksInFlightAndIsTerminal(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	srv := startScripted(t, func(int32) *protocol.Reply {
		<-ctx.Done() // never answer
		return nil
	})
	// a long idle timeout, so anything that returns promptly did so because of Close and not a deadline
	c := network.NewTCPClient(srv.addr, network.WithClientIdleTimeout(time.Minute))

	sent := make(chan error, 1)
	go func() {
		_, err := c.Send(ctx, "GET", []string{"users", "name"})
		sent <- err
	}()
	waitFor(t, "the server to receive the command", func() bool { return srv.received.Load() == 1 })

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-sent:
		if err == nil {
			t.Fatal("Send() returned nil after Close, want an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Send() did not return after Close: the in-flight read was not interrupted")
	}

	if err := c.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
	if _, err := c.Send(ctx, "GET", []string{"users", "name"}); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Send() after Close error = %v, want net.ErrClosed", err)
	}
}

// TestSend_CancelDuringBlockedRead checks that cancellation reaches a command already parked in a read, and that it
// does not rewrite history: a cancelled mutation was still sent, so its outcome is unknown rather than "not executed".
func TestSend_CancelDuringBlockedRead(t *testing.T) {
	t.Parallel()

	testCtx := t.Context()
	srv := startScripted(t, func(int32) *protocol.Reply {
		<-testCtx.Done()
		return nil
	})
	c := network.NewTCPClient(srv.addr, network.WithClientIdleTimeout(time.Minute))
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(testCtx)
	defer cancel()
	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()
	_, err := c.Send(ctx, "INCR", []string{"stats", "hits"})
	if !errors.Is(err, network.ErrOutcomeUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Send() error = %v, want both ErrOutcomeUnknown and context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("Send() took %v; cancellation did not interrupt the read", elapsed)
	}
}

// TestSend_CancelWhileQueued covers the half of cancellation that is easy to miss: waiting for a turn on the connection
// is part of the call. A caller with a short deadline must not be held for the idle timeout of whichever command got
// there first.
func TestSend_CancelWhileQueued(t *testing.T) {
	t.Parallel()

	testCtx := t.Context()
	srv := startScripted(t, func(int32) *protocol.Reply {
		<-testCtx.Done() // never answer, so the first command holds the connection
		return nil
	})
	c := network.NewTCPClient(srv.addr, network.WithClientIdleTimeout(30*time.Second))
	t.Cleanup(func() { _ = c.Close() })

	go func() { _, _ = c.Send(testCtx, "GET", []string{"users", "a"}) }()
	waitFor(t, "the first command to occupy the connection", func() bool { return srv.received.Load() == 1 })

	ctx, cancel := context.WithTimeout(testCtx, 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.Send(ctx, "GET", []string{"users", "b"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued Send() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("queued Send() took %v: waiting for a turn ignored the deadline", elapsed)
	}
}

// TestClose_InterruptsDial checks that Close reaches a command still trying to connect, not only one already using a
// socket. An unreachable address would otherwise keep the dial alive long after Close returned.
func TestClose_InterruptsDial(t *testing.T) {
	t.Parallel()

	// TEST-NET-3 (RFC 5737) is reserved and normally dropped silently, so the dial hangs rather than being refused
	c := network.NewTCPClient("203.0.113.1:9", network.WithClientIdleTimeout(time.Minute))

	sent := make(chan error, 1)
	go func() {
		_, err := c.Send(t.Context(), "SET", []string{"users", "name", "vlad"})
		sent <- err
	}()

	select {
	case err := <-sent:
		t.Skipf("this network answers the reserved address instead of dropping it (%v), so no dial hangs to interrupt", err)
	case <-time.After(500 * time.Millisecond):
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-sent:
		if err == nil {
			t.Fatal("Send() returned nil after Close, want an error")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Close() did not interrupt the dial")
	}
}

// TestSend_CancelDuringDial covers the other half of the dial window: the caller giving up rather than the client being
// closed. Connecting is part of the call, so a deadline has to bound it, and nothing was sent — a cancelled dial must
// not come back as an unknown outcome.
func TestSend_CancelDuringDial(t *testing.T) {
	t.Parallel()

	// TEST-NET-3 (RFC 5737) is reserved and normally dropped silently, so the dial hangs rather than being refused
	c := network.NewTCPClient("203.0.113.1:9", network.WithClientIdleTimeout(time.Minute))
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sent := make(chan error, 1)
	go func() {
		_, err := c.Send(ctx, "SET", []string{"users", "name", "vlad"})
		sent <- err
	}()

	select {
	case err := <-sent:
		t.Skipf("this network answers the reserved address instead of dropping it (%v), so no dial hangs to interrupt", err)
	case <-time.After(500 * time.Millisecond):
	}

	start := time.Now()
	cancel()
	select {
	case err := <-sent:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send() error = %v, want it to report the cancellation", err)
		}
		if errors.Is(err, network.ErrOutcomeUnknown) {
			t.Error("a dial that never connected reported an unknown outcome; nothing was sent")
		}
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("Send() took %v to return after cancellation", elapsed)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("cancelling the context did not interrupt the dial")
	}
}

// TestSend_DialFailureThenRedial is the allowed retry path stated positively. A refused dial happens before any byte is
// written, so it is reported as a plain failure rather than an unknown outcome, and it must not leave the client stuck:
// the next command connects to the same address once something is listening again.
func TestSend_DialFailureThenRedial(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err = ln.Close(); err != nil { // hold the address, but leave nothing listening on it
		t.Fatalf("close: %v", err)
	}

	c := network.NewTCPClient(addr, network.WithClientIdleTimeout(5*time.Second))
	t.Cleanup(func() { _ = c.Close() })

	_, err = c.Send(t.Context(), "SET", []string{"users", "name", "vlad"})
	if err == nil {
		t.Fatal("Send() to a dead address succeeded, want a connection error")
	}
	if errors.Is(err, network.ErrOutcomeUnknown) {
		t.Fatalf("Send() error = %v; a refused dial provably sent nothing", err)
	}

	var received atomic.Int32
	startServerAt(t, addr, func(_ context.Context, _ string, _ []string) protocol.Reply {
		received.Add(1)
		return protocol.SimpleString("OK")
	})

	if _, err = c.Send(t.Context(), "SET", []string{"users", "name", "vlad"}); err != nil {
		t.Fatalf("Send() after the server came up error = %v: the failed dial poisoned the client", err)
	}
	if got := received.Load(); got != 1 {
		t.Errorf("server received %d commands, want 1", got)
	}
}

// TestSend_PartialWriteRetriesMutationOnce exercises the rule from the client side: a frame the client could not finish
// writing never ran, so even a mutation is re-sent — exactly once, on a new connection.
func TestSend_PartialWriteRetriesMutationOnce(t *testing.T) {
	t.Parallel()

	var received atomic.Int32
	addr := startRude(t, &received)
	c := network.NewTCPClient(addr, network.WithClientMaxMessageSize(8<<20))
	t.Cleanup(func() { _ = c.Close() })

	// far larger than any socket buffer, so the closed peer breaks the write partway rather than swallowing it whole
	value := strings.Repeat("x", 4<<20)

	if _, err := c.Send(t.Context(), "SET", []string{"users", "blob", value}); err != nil {
		t.Fatalf("Send() error = %v, want the retry to succeed: a broken write means the command never ran", err)
	}
	if got := received.Load(); got != 1 {
		t.Errorf("server received %d complete commands, want exactly 1", got)
	}
}

// TestSend_ServerRestartMidSession is the everyday failure: a server goes away and comes back under a live client. The
// client must report while it is down and recover on its own afterwards, without being permanently poisoned.
func TestSend_ServerRestartMidSession(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	handler := func(_ context.Context, _ string, args []string) protocol.Reply {
		hits.Add(1)
		return protocol.BulkString(args[1])
	}
	addr, stop := startServerAt(t, "127.0.0.1:0", handler)

	c := network.NewTCPClient(addr)
	t.Cleanup(func() { _ = c.Close() })
	if _, err := c.Send(t.Context(), "GET", []string{"users", "name"}); err != nil {
		t.Fatalf("Send() before restart error = %v", err)
	}

	// Stopping closes the listener, but a connection already established still serves a command before the server lets it
	// go, so it takes a couple of attempts before the client is really talking to nothing.
	stop()
	var downErr error
	for range 5 {
		if _, downErr = c.Send(t.Context(), "GET", []string{"users", "name"}); downErr != nil {
			break
		}
	}
	if downErr == nil {
		t.Fatal("Send() kept succeeding after the server stopped")
	}

	startServerAt(t, addr, handler)
	resp, err := c.Send(t.Context(), "GET", []string{"users", "name"})
	if err != nil {
		t.Fatalf("Send() after restart error = %v: the client did not recover", err)
	}
	if resp.Value != "name" {
		t.Errorf("reply = %q, want %q", resp.Value, "name")
	}
}

// TestSend_Concurrent pins the serialization. A clean race report is not enough on its own: interleaved frames or a
// stolen reply show up as a goroutine receiving the answer to someone else's command, which the echoed key detects.
func TestSend_Concurrent(t *testing.T) {
	t.Parallel()

	addr := startServer(t, func(_ context.Context, _ string, args []string) protocol.Reply {
		return protocol.BulkString(args[1])
	})
	c := network.NewTCPClient(addr)
	t.Cleanup(func() { _ = c.Close() })

	const goroutines, perGoroutine = 8, 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)

	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				key := fmt.Sprintf("g%d-i%d", g, i)
				resp, err := c.Send(t.Context(), "GET", []string{"users", key})
				if err != nil {
					errs <- fmt.Errorf("Send %s: %w", key, err)
					return
				}
				if resp.Value != key {
					errs <- fmt.Errorf("reply for %s was %q: a reply crossed between commands", key, resp.Value)
					return
				}
			}
		})
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestServer_PartialFrameNeverDispatches guards the premise the client's retry rule rests on: a command the client
// could not finish writing must never run, which is what makes re-sending a mutation after a write error safe. If a
// future change decodes commands incrementally this fails, and that rule has to be revisited.
func TestServer_PartialFrameNeverDispatches(t *testing.T) {
	t.Parallel()

	var dispatched atomic.Int32
	addr := startServer(t, func(context.Context, string, []string) protocol.Reply {
		dispatched.Add(1)
		return protocol.SimpleString("OK")
	})

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// a SET frame announcing four arguments but stopping one short
	if _, err = io.WriteString(conn, "*4\r\n$3\r\nSET\r\n$5\r\nusers\r\n$4\r\nname\r\n"); err != nil {
		t.Fatalf("write truncated frame: %v", err)
	}
	if err = conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// a complete command proves the server is alive and gives the truncated connection time to be torn down; only this
	// command may have reached the handler
	c := network.NewTCPClient(addr)
	t.Cleanup(func() { _ = c.Close() })
	if _, err = c.Send(t.Context(), "SET", []string{"users", "name", "vlad"}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if got := dispatched.Load(); got != 1 {
		t.Errorf("handler ran %d times, want 1: the truncated frame must never be dispatched", got)
	}
}
