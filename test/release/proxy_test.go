//go:build release && linux

package release_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	lostReply      = "lost-reply"      // the command runs; the client gets EOF instead of the reply
	partialReply   = "partial-reply"   // the command runs; the client gets the first byte of the reply, then EOF
	partialRequest = "partial-request" // the server gets the frame minus its last byte, then EOF
)

// faultProxy faults the first command it relays and serves later connections normally, so a client that transparently
// re-sends a mutation is counted instead of failing some other way.
type faultProxy struct {
	addr, upstream, mode string
	ln                   net.Listener
	commands             atomic.Int32
	wg                   sync.WaitGroup
	mu                   sync.Mutex
	errs                 []error
}

func startProxy(t *testing.T, upstream, mode string) *faultProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{addr: ln.Addr().String(), upstream: upstream, mode: mode, ln: ln}
	ctx := t.Context()
	p.wg.Go(func() { p.serve(ctx) })
	t.Cleanup(func() {
		_ = ln.Close()
		p.wg.Wait()
	})
	return p
}

// close stops the proxy, fails the test if relaying went wrong, and reports how many commands reached it.
func (p *faultProxy) close(t *testing.T) int32 {
	t.Helper()
	_ = p.ln.Close()
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.errs) > 0 {
		t.Fatalf("proxy failed: %v", errors.Join(p.errs...))
	}
	return p.commands.Load()
}

// serve handles one connection at a time, so a retry is only accepted once the faulted exchange is over.
func (p *faultProxy) serve(ctx context.Context) {
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		if err = p.relay(ctx, client); err != nil {
			p.mu.Lock()
			p.errs = append(p.errs, err)
			p.mu.Unlock()
		}
		_ = client.Close()
	}
}

func (p *faultProxy) relay(ctx context.Context, client net.Conn) error {
	if err := client.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		return err
	}
	request, err := decode(bufio.NewReader(client))
	if err != nil {
		return fmt.Errorf("read client command: %w", err)
	}
	args, ok := request.([]any)
	if !ok {
		return fmt.Errorf("client sent %#v, not a command array", request)
	}
	words := make([]string, len(args))
	for i, arg := range args {
		if words[i], ok = arg.(string); !ok {
			return fmt.Errorf("command argument %d is %#v", i, arg)
		}
	}
	first := p.commands.Add(1) == 1

	server, err := dialConn(ctx, p.upstream)
	if err != nil {
		return err
	}
	defer server.Close()
	if first && p.mode == partialRequest {
		return truncate(server, encode(words...))
	}
	reply, err := server.do(words...)
	if err != nil {
		return fmt.Errorf("relay %v: %w", words, err)
	}
	switch {
	case !first:
		_, err = client.Write(encodeReply(reply))
		return err
	case reply != "1":
		return fmt.Errorf("unexpected mutation reply %#v", reply)
	case p.mode == partialReply:
		_, err = client.Write([]byte(":"))
		return err
	default:
		return nil
	}
}

// truncate sends all but the last byte of frame and closes the write side, so the server never decodes it whole.
func truncate(server *conn, frame []byte) error {
	tcp, ok := server.Conn.(*net.TCPConn)
	if !ok {
		return errors.New("upstream connection is not TCP")
	}
	if err := tcp.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		return err
	}
	if _, err := tcp.Write(frame[:len(frame)-1]); err != nil {
		return err
	}
	if err := tcp.CloseWrite(); err != nil {
		return err
	}
	buf := make([]byte, 4096)
	if n, _ := tcp.Read(buf); n > 0 && buf[0] != '-' {
		return fmt.Errorf("partial request executed: %q", buf[:n])
	}
	return nil
}

// encodeReply re-encodes a relayed reply. Only the shapes a single-key command can produce are needed.
func encodeReply(reply any) []byte {
	switch v := reply.(type) {
	case string:
		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(v), v)
	case int64:
		return fmt.Appendf(nil, ":%d\r\n", v)
	case respError:
		return fmt.Appendf(nil, "-%s\r\n", v)
	default:
		return []byte("$-1\r\n")
	}
}
