package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/status"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startServer runs an in-process standalone server, wired like cmd/db's, and returns its address.
func startServer(t *testing.T) string {
	t.Helper()
	logger := slog.New(slog.DiscardHandler)
	srv, err := network.NewTCPServer("127.0.0.1:0", logger)
	require.NoError(t, err)
	store := storage.New(engine.New(), storage.WithListLimit(4096))
	comp := compute.New(parser.New(), store, logger,
		compute.WithStatus(status.New(engine.TypeInMemory, status.DurabilityEphemeral, store, nil)))
	done := make(chan error, 1)
	go func() { done <- srv.Serve(comp.Handle) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.NoError(t, srv.Shutdown(ctx))
		assert.NoError(t, <-done)
	})
	return srv.Addr().String()
}

type result struct {
	code   int
	stdout string
	stderr string
}

func runCLI(t *testing.T, input string, args ...string) result {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(args, strings.NewReader(input), &stdout, &stderr, false)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func example(t *testing.T, name string) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("..", "..", "examples", name))
	require.NoError(t, err)
	return string(content)
}

// TestSmokeFileExitsZero is the contract examples/smoke.txt documents: every command in it succeeds on a standalone
// server, so a script can gate on the exit status. It runs twice to show the file is safe to repeat.
func TestSmokeFileExitsZero(t *testing.T) {
	t.Parallel()
	addr := startServer(t)
	for range 2 {
		got := runCLI(t, example(t, "smoke.txt"), "-address", addr)
		require.Equal(t, exitOK, got.code, "stderr: %s", got.stderr)
		require.Empty(t, got.stderr)
		// Piped input gets neither banner nor prompt, so stdout is only the replies.
		assert.True(t, strings.HasPrefix(got.stdout, "PONG\nready\ntrue\n"), "stdout: %q", got.stdout)
		assert.NotContains(t, got.stdout, "> ")
		assert.NotContains(t, got.stdout, "Available commands")
	}
}

// TestErrorsFileExitsNonzero runs every deliberate failure in examples/errors.txt: each is reported with its line
// number and code, the run continues past it, and the exit status says commands failed.
func TestErrorsFileExitsNonzero(t *testing.T) {
	t.Parallel()
	addr := startServer(t)
	got := runCLI(t, example(t, "errors.txt"), "-address", addr)
	require.Equal(t, exitFailure, got.code)

	lines := strings.Split(strings.TrimSpace(got.stderr), "\n")
	codes := make([]string, 0, len(lines))
	for _, line := range lines {
		require.True(t, strings.HasPrefix(line, "line "), "error line without its line number: %q", line)
		_, rest, _ := strings.Cut(line, ": ")
		code, _, _ := strings.Cut(rest, " ")
		codes = append(codes, code)
	}
	assert.Equal(t, []string{
		"ARITY", "ARITY", "UNKNOWNCMD", "ARGUMENT", "ARGUMENT", "ARGUMENT",
		"WRONGTYPE", "WRONGTYPE", "UNAVAILABLE", "UNAVAILABLE",
	}, codes, "stderr: %s", got.stderr)
	// The setup commands before the failures still ran and printed their replies.
	assert.Equal(t, "OK\n1\n", got.stdout)
}

func TestQuietPrintsOnlyErrors(t *testing.T) {
	t.Parallel()
	addr := startServer(t)

	got := runCLI(t, example(t, "smoke.txt"), "-q", "-address", addr)
	require.Equal(t, exitOK, got.code, "stderr: %s", got.stderr)
	assert.Empty(t, got.stdout)
	assert.Empty(t, got.stderr)

	got = runCLI(t, "SET t k v\nINCR t k\nGET t k\n", "-q", "-address", addr)
	require.Equal(t, exitFailure, got.code)
	assert.Empty(t, got.stdout)
	assert.Equal(t, "line 2: WRONGTYPE wrong type: key holds string, INCR requires int or float\n", got.stderr)
}

// TestSessionEndingFailures covers the failures that stop the run rather than continuing: a line the CLI cannot parse
// and a server it cannot reach. Both exit nonzero, and nothing after them is sent.
func TestSessionEndingFailures(t *testing.T) {
	t.Parallel()
	addr := startServer(t)

	got := runCLI(t, "SET t k v\nSET t k 'unterminated\nSET t after v\n", "-address", addr)
	require.Equal(t, exitFailure, got.code)
	assert.Contains(t, got.stderr, "line 2: Failed to send command: unterminated quoted string")
	verify := runCLI(t, "GET t after\n", "-address", addr)
	assert.Equal(t, "not found\n", verify.stdout, "the line after the failure must not have run")

	lc := net.ListenConfig{}
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closed := listener.Addr().String()
	require.NoError(t, listener.Close())
	got = runCLI(t, "PING\nGET t k\n", "-q", "-address", closed)
	require.Equal(t, exitFailure, got.code)
	assert.Equal(t, 1, strings.Count(got.stderr, "Failed to send command"), "stderr: %s", got.stderr)
}

func TestExitAndEmptyInput(t *testing.T) {
	t.Parallel()
	addr := startServer(t)

	got := runCLI(t, "PING\nexit\nUNKNOWN\n", "-address", addr)
	require.Equal(t, exitOK, got.code, "nothing after exit runs")
	assert.Equal(t, "PONG\n", got.stdout)

	got = runCLI(t, "", "-address", addr)
	require.Equal(t, exitOK, got.code)
}

func TestUsageErrors(t *testing.T) {
	t.Parallel()
	assert.Equal(t, exitUsage, runCLI(t, "", "-no-such-flag").code)
	assert.Equal(t, exitUsage, runCLI(t, "", "GET", "t", "k").code, "commands come from stdin, not arguments")
	assert.Equal(t, exitUsage, runCLI(t, "", "-config", filepath.Join(t.TempDir(), "missing.yaml")).code)
	assert.Equal(t, exitOK, runCLI(t, "", "-version").code)
}

func TestInteractiveSessionPrompts(t *testing.T) {
	t.Parallel()
	addr := startServer(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"-address", addr}, strings.NewReader("PING\nNOPE\n"), &stdout, &stderr, true)
	require.Equal(t, exitFailure, code)
	assert.Contains(t, stdout.String(), "Available commands:")
	assert.Contains(t, stdout.String(), "> PONG\n")
	// At a prompt the error follows the command that caused it, so it needs no line number.
	assert.Equal(t, "UNKNOWNCMD unknown command: NOPE\n", stderr.String())
}
