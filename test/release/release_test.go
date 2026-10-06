//go:build release && linux

// Package release_test holds the black-box release checks. They run the db and db-cli binaries of an extracted release
// archive (RELEASE_ROOT) and talk to them only through the CLI and raw TCP. Nothing here imports the implementation:
// the RESP codec below is written independently of internal/protocol, so a bug shared by the server's encoder and
// decoder cannot hide behind the same code in the checks. Run them through test/release/run.sh, which verifies the
// archive's checksum first.
package release_test

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	// maxConnections and maxMessageKB are written into every server configuration; the limit checks derive from them.
	maxConnections = 16
	maxMessageKB   = 64
	ioTimeout      = 3 * time.Second
	shutdownBound  = 5 * time.Second
)

type (
	// null is a RESP null bulk string or array.
	null struct{}
	// respError is a RESP error reply: a wire code, a space and the message.
	respError string
)

func releaseRoot(t *testing.T) string {
	t.Helper()
	root := os.Getenv("RELEASE_ROOT")
	if root == "" {
		t.Fatal("RELEASE_ROOT must name an extracted release archive; run test/release/run.sh")
	}
	return root
}

// goldenDir is the frozen v1 fixture directory of this checkout. The fixtures are never regenerated for a frozen
// version, so every release must read them exactly.
func goldenDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../../internal/compat/testdata/golden/v1")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func encode(args ...string) []byte {
	frame := fmt.Appendf(nil, "*%d\r\n", len(args))
	for _, arg := range args {
		frame = fmt.Appendf(frame, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return frame
}

// decode reads one reply: a string for simple and bulk strings, int64, respError, null, or []any for arrays.
func decode(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	body, ok := strings.CutSuffix(line, "\r\n")
	if !ok || body == "" {
		return nil, fmt.Errorf("malformed RESP line %q", line)
	}
	switch kind, rest := body[0], body[1:]; kind {
	case '+':
		return rest, nil
	case '-':
		return respError(rest), nil
	case ':':
		n, parseErr := strconv.ParseInt(rest, 10, 64)
		return n, parseErr
	case '$':
		return decodeBulk(r, rest)
	case '*':
		return decodeArray(r, rest)
	default:
		return nil, fmt.Errorf("unknown RESP type %q", kind)
	}
}

func decodeBulk(r *bufio.Reader, size string) (any, error) {
	n, err := strconv.Atoi(size)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return null{}, nil
	}
	data := make([]byte, n+2)
	if _, err = io.ReadFull(r, data); err != nil {
		return nil, err
	}
	if !bytes.HasSuffix(data, []byte("\r\n")) {
		return nil, errors.New("bulk string is not terminated by CRLF")
	}
	return string(data[:n]), nil
}

func decodeArray(r *bufio.Reader, size string) (any, error) {
	n, err := strconv.Atoi(size)
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return null{}, nil
	}
	items := make([]any, 0, n)
	for range n {
		item, itemErr := decode(r)
		if itemErr != nil {
			return nil, itemErr
		}
		items = append(items, item)
	}
	return items, nil
}

func mustDecode(t *testing.T, r *bufio.Reader) any {
	t.Helper()
	reply, err := decode(r)
	if err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	return reply
}

// conn is one client connection. Every command gets a fresh I/O deadline.
type conn struct {
	net.Conn

	r *bufio.Reader
}

func dialConn(ctx context.Context, addr string) (*conn, error) {
	c, err := (&net.Dialer{Timeout: ioTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &conn{Conn: c, r: bufio.NewReader(c)}, nil
}

func dial(t *testing.T, addr string) *conn {
	t.Helper()
	c, err := dialConn(t.Context(), addr)
	if err != nil {
		t.Fatalf("connect to %s: %v", addr, err)
	}
	return c
}

func (c *conn) do(args ...string) (any, error) {
	if err := c.SetDeadline(time.Now().Add(ioTimeout)); err != nil {
		return nil, err
	}
	if _, err := c.Write(encode(args...)); err != nil {
		return nil, err
	}
	return decode(c.r)
}

func (c *conn) must(t *testing.T, args ...string) any {
	t.Helper()
	reply, err := c.do(args...)
	if err != nil {
		t.Fatalf("%s: %v", strings.Join(args, " "), err)
	}
	return reply
}

// call sends one command on a connection of its own.
func call(t *testing.T, addr string, args ...string) any {
	t.Helper()
	c := dial(t, addr)
	defer c.Close()
	return c.must(t, args...)
}

func atoi(t *testing.T, reply any) int64 {
	t.Helper()
	switch v := reply.(type) {
	case int64:
		return v
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	t.Fatalf("not an integer reply: %#v", reply)
	return 0
}

func strs(t *testing.T, reply any) []string {
	t.Helper()
	items, ok := reply.([]any)
	if !ok {
		t.Fatalf("not an array reply: %#v", reply)
	}
	out := make([]string, len(items))
	for i, item := range items {
		if out[i], ok = item.(string); !ok {
			t.Fatalf("array element %d is not a string: %#v", i, item)
		}
	}
	return out
}

type serverConfig struct {
	Data     string // data directory; defaults to a fresh one
	Engine   string // "in_memory" (with the WAL) or "tiered"
	Policy   string // WAL sync policy
	Snapshot string // WAL snapshot interval
}

// server is one db process started from the release archive. It belongs to the test that created it and is killed
// when that test ends.
type server struct {
	t                  *testing.T
	work, data, config string
	addr               string
	cmd                *exec.Cmd
	log                *os.File
	exited             chan struct{}
}

func newServer(t *testing.T, cfg serverConfig) *server {
	t.Helper()
	work := t.TempDir()
	s := &server{t: t, work: work, data: cfg.Data, config: filepath.Join(work, "server.yaml"), addr: freeAddr(t)}
	if s.data == "" {
		s.data = filepath.Join(work, "data")
	}
	writeFile(t, s.config, s.configText(cfg))
	t.Cleanup(func() { s.stop(true) })
	return s
}

func (s *server) configText(cfg serverConfig) string {
	var storage string
	if cfg.Engine == "tiered" {
		storage = fmt.Sprintf("engine:\n  type: tiered\n  data_dir: %q\n", s.data)
	} else {
		policy, snapshot := cmp.Or(cfg.Policy, "always"), cmp.Or(cfg.Snapshot, "1h")
		storage = fmt.Sprintf("engine:\n  type: in_memory\nwal:\n  enabled: true\n  data_dir: %q\n  sync: %q\n"+
			"  segment_size: 1\n  snapshot_interval: %s\n", s.data, policy, snapshot)
	}
	return storage + fmt.Sprintf("network:\n  address: %q\n  max_connections: %d\n  max_message_size: %d\n"+
		"  idle_timeout: 10s\n  shutdown_timeout: 1s\nlogging:\n  level: error\n", s.addr, maxConnections, maxMessageKB)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// start runs the server and waits until it answers PING.
func (s *server) start() {
	s.t.Helper()
	log, err := os.OpenFile(filepath.Join(s.work, "server.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		s.t.Fatal(err)
	}
	cmd := command(s.t.Context(), filepath.Join(releaseRoot(s.t), "db"), "-config", s.config)
	cmd.Dir, cmd.Stdout, cmd.Stderr = s.work, log, log
	if err = cmd.Start(); err != nil {
		_ = log.Close()
		s.t.Fatalf("start server: %v", err)
	}
	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	s.cmd, s.log, s.exited = cmd, log, exited
	eventually(s.t, func() bool {
		select {
		case <-exited:
			s.t.Fatalf("server exited during startup: %s", s.logText())
		default:
		}
		return s.answers()
	})
}

// stop sends SIGTERM (or SIGKILL) and waits. A SIGTERM shutdown must finish within the bound and exit 0.
func (s *server) stop(kill bool) {
	s.t.Helper()
	if s.cmd == nil {
		return
	}
	cmd, exited := s.cmd, s.exited
	s.cmd = nil
	defer s.log.Close()
	sig := syscall.SIGTERM
	if kill {
		sig = syscall.SIGKILL
	}
	_ = cmd.Process.Signal(sig)
	select {
	case <-exited:
	case <-time.After(shutdownBound):
		_ = cmd.Process.Kill()
		<-exited
		s.t.Fatalf("SIGTERM exceeded the %s shutdown bound", shutdownBound)
	}
	if code := cmd.ProcessState.ExitCode(); !kill && code != 0 {
		s.t.Fatalf("SIGTERM exit %d: %s", code, s.logText())
	}
}

func (s *server) answers() bool {
	c, err := dialConn(s.t.Context(), s.addr)
	if err != nil {
		return false
	}
	defer c.Close()
	reply, err := c.do("PING")
	return err == nil && reply == "PONG"
}

func (s *server) pid() int { return s.cmd.Process.Pid }

func (s *server) logText() string {
	data, _ := os.ReadFile(filepath.Join(s.work, "server.log"))
	return string(data)
}

func (s *server) call(args ...string) any {
	s.t.Helper()
	return call(s.t, s.addr, args...)
}

func (s *server) status() map[string]string {
	s.t.Helper()
	items := strs(s.t, s.call("STATUS"))
	fields := make(map[string]string, len(items)/2)
	for i := 0; i+1 < len(items); i += 2 {
		fields[items[i]] = items[i+1]
	}
	return fields
}

// cli pipes input to the archive's db-cli, connected directly to the server, or through clientConfig when set.
func (s *server) cli(input, clientConfig string) result {
	s.t.Helper()
	args, dir := []string{"-timeout", "2s", "-address", s.addr}, s.work
	if clientConfig != "" {
		args, dir = []string{"-timeout", "2s", "-config", filepath.Base(clientConfig)}, filepath.Dir(clientConfig)
	}
	return run(s.t, dir, input, 15*time.Second, filepath.Join(releaseRoot(s.t), "db-cli"), args...)
}

type result struct {
	stdout, stderr string
	code           int
}

// command builds a child process that the kernel kills when the test binary dies. A timed-out test panics and exits
// without running cleanups or cancelling contexts, which would otherwise leave servers running.
func command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	return cmd
}

// run executes a binary and fails the test if it does not exit within timeout.
func run(t *testing.T, dir, input string, timeout time.Duration, name string, args ...string) result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	cmd := command(ctx, name, args...)
	var stdout, stderr strings.Builder
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = dir, strings.NewReader(input), &stdout, &stderr
	err := cmd.Run()
	res := result{stdout: stdout.String(), stderr: stderr.String()}
	if ctx.Err() != nil {
		t.Fatalf("%s %v did not exit within %s: %s%s", filepath.Base(name), args, timeout, res.stdout, res.stderr)
	}
	if exitErr, ok := errors.AsType[*exec.ExitError](err); ok {
		res.code = exitErr.ExitCode()
	} else if err != nil {
		t.Fatalf("run %s: %v", filepath.Base(name), err)
	}
	return res
}

// refused starts the archive's server with config and requires it to exit with an error, returning its output.
func refused(t *testing.T, config, dir string) string {
	t.Helper()
	res := run(t, dir, "", shutdownBound, filepath.Join(releaseRoot(t), "db"), "-config", config)
	if res.code <= 0 {
		t.Fatalf("startup was not refused (exit %d): %s%s", res.code, res.stdout, res.stderr)
	}
	return res.stdout + res.stderr
}

// hashes maps every file under dir to its SHA-256, except the empty directory-lock file.
func hashes(t *testing.T, dir string) map[string]string {
	t.Helper()
	sums := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() || d.Name() == ".db.lock" {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		sums[rel] = hex.EncodeToString(sum[:])
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return sums
}

// eventually polls condition for up to ten seconds.
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	const timeout = 10 * time.Second
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if condition() {
			return
		}
	}
	t.Fatalf("condition not met within %s", timeout)
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatal(err)
	}
}

// onlyFile returns the single file matching pattern.
func onlyFile(t *testing.T, pattern string) string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) != 1 {
		t.Fatalf("want exactly one %s, got %v (%v)", pattern, matches, err)
	}
	return matches[0]
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
