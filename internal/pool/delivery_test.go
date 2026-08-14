package pool_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/pool"
	"github.com/OutOfStack/db/internal/protocol"
)

// silentServer reads a command in full and then closes without replying, which is what a server that commits and dies
// before answering looks like. A network.RequestHandler cannot express this: it can only return a reply.
type silentServer struct {
	addr     string
	received atomic.Int32
}

func startSilent(t *testing.T) *silentServer {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	s := &silentServer{addr: ln.Addr().String()}
	go func() {
		for {
			conn, aErr := ln.Accept()
			if aErr != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				if _, _, rErr := protocol.ReadCommand(bufio.NewReader(conn), 4096); rErr == nil {
					s.received.Add(1)
				}
			}()
		}
	}()
	return s
}

// deadAddr returns an address nothing is listening on, so connecting to it fails immediately.
func deadAddr(t *testing.T) string {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err = ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

// TestClient_OutcomeUnknownDoesNotFailOver is the most dangerous behavior the pool can get wrong. When a mutation may
// already have been applied, re-sending it applies it twice, so an unknown outcome has to end the call rather than start
// a retry. A pool holds at most one master, so "elsewhere" is the same master again — and the standby, which must not
// receive a write at all.
func TestClient_OutcomeUnknownDoesNotFailOver(t *testing.T) {
	t.Parallel()

	silent := startSilent(t)
	var standbyHits atomic.Int32
	standbyAddr := startHandler(t, okHandler(&standbyHits))

	client := newPool(t, []pool.ServerConfig{
		{Address: silent.addr, Role: pool.RoleMaster},
		{Address: standbyAddr, Role: pool.RoleStandby},
	}, pool.StrategyMasterFirst)

	_, err := client.Send(t.Context(), "INCR", []string{"stats", "hits"})
	if !errors.Is(err, network.ErrOutcomeUnknown) {
		t.Fatalf("Send() error = %v, want ErrOutcomeUnknown", err)
	}
	// the pool is configured for retries, so anything above 1 means the INCR was applied more than once
	if got := silent.received.Load(); got != 1 {
		t.Errorf("master received %d commands, want 1", got)
	}
	if got := standbyHits.Load(); got != 0 {
		t.Errorf("standby received %d commands, want 0: the mutation must not be replayed elsewhere", got)
	}
}

// TestClient_ReadsStillFailOver guards the other direction: only mutations are pinned to one server. A read whose reply
// is lost has no side effects, so the pool must still route it to a healthy server.
func TestClient_ReadsStillFailOver(t *testing.T) {
	t.Parallel()

	silent := startSilent(t)
	var backupHits atomic.Int32
	backupAddr := startHandler(t, okHandler(&backupHits))

	client := newPool(t, []pool.ServerConfig{
		{Address: silent.addr, Role: pool.RoleMaster},
		{Address: backupAddr, Role: pool.RoleStandby},
	}, pool.StrategyMasterFirst)

	if _, err := client.Send(t.Context(), "GET", []string{"users", "name"}); err != nil {
		t.Fatalf("Send() error = %v, want the read to fail over", err)
	}
	if got := backupHits.Load(); got == 0 {
		t.Error("the read never reached the healthy server")
	}
}

// TestClient_CancelDoesNotQuarantineServer pins what a cancelled call may and may not touch. Giving up on a request is
// a statement about the caller, not about the server, so it must leave selector health alone — otherwise one caller
// with a tight deadline takes a healthy master out of rotation for the whole failure timeout and later reads silently
// drift to a standby.
func TestClient_CancelDoesNotQuarantineServer(t *testing.T) {
	t.Parallel()

	var masterHits, standbyHits atomic.Int32
	masterAddr := startHandler(t, func(_ context.Context, _ string, _ []string) protocol.Reply {
		if masterHits.Add(1) == 1 {
			time.Sleep(500 * time.Millisecond) // outlast the first caller's deadline, then answer normally
		}
		return protocol.SimpleString("OK")
	})
	standbyAddr := startHandler(t, okHandler(&standbyHits))

	client := newPool(t, []pool.ServerConfig{
		{Address: masterAddr, Role: pool.RoleMaster},
		{Address: standbyAddr, Role: pool.RoleStandby},
	}, pool.StrategyMasterFirst)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := client.Send(ctx, "GET", []string{"users", "name"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want context.DeadlineExceeded", err)
	}

	// master_first sends reads to the master unless it is marked failed, so where this one lands is the assertion
	if _, err := client.Send(t.Context(), "GET", []string{"users", "name"}); err != nil {
		t.Fatalf("second Send() error = %v", err)
	}
	if got := standbyHits.Load(); got != 0 {
		t.Errorf("standby received %d commands, want 0: the cancelled call quarantined a healthy master", got)
	}
	if got := masterHits.Load(); got != 2 {
		t.Errorf("master received %d commands, want 2", got)
	}
}

// newDeadPool builds a pool of unreachable servers with a long retry delay, so a test can prove that something other
// than the delay itself (context cancellation, Close) is what ends the call.
func newDeadPool(t *testing.T, retryDelay time.Duration) *pool.Client {
	t.Helper()

	return newPool(t, []pool.ServerConfig{
		{Address: deadAddr(t), Role: pool.RoleMaster},
		{Address: deadAddr(t), Role: pool.RoleStandby},
	}, pool.StrategyMasterFirst, retryDelay)
}

// TestClient_CancelDuringBackoff verifies the retry delay honors the caller's context instead of sleeping through it.
func TestClient_CancelDuringBackoff(t *testing.T) {
	t.Parallel()

	// long enough that sleeping through even one retry delay would fail the test
	client := newDeadPool(t, 30*time.Second)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := client.Send(ctx, "GET", []string{"users", "name"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Send() took %v; the retry delay ignored the context", elapsed)
	}
}

// TestClient_CloseInterruptsBackoff covers the lifecycle promise during a retry delay. The delay is configurable and can
// be far longer than a caller expects a closed pool to keep working, so Close has to reach a call parked in one.
func TestClient_CloseInterruptsBackoff(t *testing.T) {
	t.Parallel()

	// sleeping through even one retry delay would blow the deadline below
	client := newDeadPool(t, 30*time.Second)

	sent := make(chan error, 1)
	go func() {
		_, sErr := client.Send(t.Context(), "GET", []string{"users", "name"})
		sent <- sErr
	}()
	time.Sleep(100 * time.Millisecond) // let the first attempt fail and the call settle into its backoff

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case sErr := <-sent:
		if sErr == nil {
			t.Fatal("Send() returned nil after Close, want an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close() did not interrupt the retry delay")
	}
}

// TestClient_ConcurrentSendAndClose runs commands against a pool while it is closed underneath them. Nothing may panic
// or hang, and every call has to end in either a real reply or an error.
func TestClient_ConcurrentSendAndClose(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	client := newPool(t, []pool.ServerConfig{
		{Address: startHandler(t, okHandler(&hits)), Role: pool.RoleMaster},
		{Address: startHandler(t, okHandler(&hits)), Role: pool.RoleStandby},
	}, pool.StrategyRoundRobin)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				_, _ = client.Send(t.Context(), "GET", []string{"users", "name"})
			}
		})
	}

	time.Sleep(100 * time.Millisecond)
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	close(done)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(15 * time.Second):
		t.Fatal("commands did not finish after the pool was closed")
	}
}

// TestClient_CloseIsTerminal pins that a closed pool stays closed rather than transparently rebuilding connections.
func TestClient_CloseIsTerminal(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	client := newPool(t, []pool.ServerConfig{
		{Address: startHandler(t, okHandler(&hits)), Role: pool.RoleMaster},
	}, pool.StrategyMasterFirst)

	if _, err := client.Send(t.Context(), "SET", []string{"t", "k", "v"}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}

	if _, err := client.Send(t.Context(), "SET", []string{"t", "k", "v"}); !errors.Is(err, net.ErrClosed) {
		t.Errorf("Send() after Close error = %v, want net.ErrClosed", err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("server received %d commands, want 1: the closed pool reconnected", got)
	}
}
