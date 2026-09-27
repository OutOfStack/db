package client_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/status"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
)

// TestServerErrorCodeMatrix walks every failure the server can report through a real connection and checks the wire
// code the client sees. The codes are the v1 contract — callers branch on them instead of on message text — so each one
// needs a live example, and a command whose failure silently changed code fails here.
func TestServerErrorCodeMatrix(t *testing.T) {
	t.Parallel()

	addr := startServer(t)
	c := mustClient(t, addr)
	ctx := t.Context()

	// Fixtures the failing commands are aimed at: a string that is not a number, an array, and a map.
	seed(t, c, ctx, "users", "name", "vlad")
	seed(t, c, ctx, "users", "tags", "[1,2]")
	seed(t, c, ctx, "users", "profile", `{"city":"berlin"}`)
	seed(t, c, ctx, "users", "overflow", "9223372036854775807")

	tests := []struct {
		name    string
		command string
		code    string
	}{
		{name: "unknown command", command: "NOPE users key", code: client.CodeUnknownCommand},
		{name: "too few arguments", command: "GET users", code: client.CodeArity},
		{name: "too many arguments", command: "GET users key extra", code: client.CodeArity},
		{name: "empty table", command: `GET "" key`, code: client.CodeArgument},
		{name: "empty key", command: `GET users ""`, code: client.CodeArgument},
		{name: "unparsable value literal", command: `SET users broken [1,`, code: client.CodeArgument},
		{name: "non-numeric INCR delta", command: "INCR users counter abc", code: client.CodeArgument},
		{name: "malformed REPLICATION subcommand", command: "REPLICATION NOPE", code: client.CodeArgument},
		{name: "table name too long", command: "GET " + strings.Repeat("a", 129) + " key", code: client.CodeTooLarge},
		{name: "INCR on a string", command: "INCR users name", code: client.CodeWrongType},
		{name: "APPEND to a non-array", command: "APPEND users name 1", code: client.CodeWrongType},
		{name: "HSET on a non-map", command: "HSET users name city berlin", code: client.CodeWrongType},
		{name: "HGET on a non-map", command: "HGET users tags city", code: client.CodeWrongType},
		{name: "INCR overflow", command: "INCR users overflow 9223372036854775807", code: client.CodeWrongType},
		{name: "replication not enabled", command: "REPLICATION STATUS", code: client.CodeUnavailable},
		{name: "PROMOTE without replication", command: "PROMOTE", code: client.CodeUnavailable},
		{name: "PING with an argument", command: "PING hello", code: client.CodeArity},
		{name: "STATUS with an argument", command: "STATUS verbose", code: client.CodeArity},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireCode(t, rawErr(t, c, t.Context(), tc.command), tc.code)
		})
	}
}

// TestServerErrorCodeReadOnly covers the standby refusal, which needs a storage the plain harness cannot build.
func TestServerErrorCodeReadOnly(t *testing.T) {
	t.Parallel()

	addr, _ := startServerWithStorage(t, storage.New(engine.New(), storage.WithReadOnly(true)))
	c := mustClient(t, addr)

	err := c.Set(t.Context(), "users", "name", "vlad")
	requireCode(t, err, client.CodeReadOnly)
	// A read is still served.
	if _, gErr := c.Get(t.Context(), "users", "name"); !errors.Is(gErr, client.ErrNotFound) {
		t.Errorf("Get on a read-only server error = %v, want ErrNotFound", gErr)
	}
}

// TestServerErrorCodeUnavailable covers a fenced storage, which refuses reads as well as writes.
func TestServerErrorCodeUnavailable(t *testing.T) {
	t.Parallel()

	store := storage.New(engine.New())
	store.Fence(errors.New("test fence"))
	addr, _ := startServerWithStorage(t, store)
	c := mustClient(t, addr)

	requireCode(t, c.Set(t.Context(), "users", "name", "vlad"), client.CodeUnavailable)
	_, err := c.Get(t.Context(), "users", "name")
	requireCode(t, err, client.CodeUnavailable)
}

// TestErrWrongTypeSentinel checks the one code with an exported sentinel: errors.Is must match it, and must not match a
// server error of another code.
func TestErrWrongTypeSentinel(t *testing.T) {
	t.Parallel()

	c := mustClient(t, startServer(t))
	ctx := t.Context()
	seed(t, c, ctx, "users", "name", "vlad")

	if _, err := c.Incr(ctx, "users", "name", "1"); !errors.Is(err, client.ErrWrongType) {
		t.Errorf("Incr on a string error = %v, want ErrWrongType", err)
	}
	if _, err := c.Get(ctx, "users", "name"); errors.Is(err, client.ErrWrongType) {
		t.Error("a successful Get matched ErrWrongType")
	}
	if _, err := c.Raw(ctx, "NOPE"); errors.Is(err, client.ErrWrongType) {
		t.Error("an unknown-command error matched ErrWrongType")
	}
}

// TestRawRendersArrays freezes how Raw renders each reply kind. The rendering is documented on the method, so it is
// part of the contract rather than an implementation detail.
func TestRawRendersArrays(t *testing.T) {
	t.Parallel()

	c := mustClient(t, startServer(t))
	ctx := t.Context()
	seed(t, c, ctx, "users", "name", "vlad")
	seed(t, c, ctx, "orders", "id", "7")

	tests := []struct{ name, command, want string }{
		{name: "simple string", command: "SET users city berlin", want: "OK"},
		{name: "bulk string", command: "GET users name", want: "vlad"},
		{name: "integer", command: "APPEND users list 1", want: "1"},
		{name: "null", command: "GET users missing", want: "not found"},
		{name: "array joined by newline", command: "TABLES", want: "orders\nusers"},
		{name: "empty array", command: "KEYS missing", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := c.Raw(ctx, tc.command)
			if err != nil {
				t.Fatalf("Raw(%s) error = %v", tc.command, err)
			}
			if got != tc.want {
				t.Errorf("Raw(%s) = %q, want %q", tc.command, got, tc.want)
			}
		})
	}
}

// TestServerErrorCodeProtocol covers the one code no typed client can provoke: a malformed frame, which the server
// answers before closing the connection. It is sent over a raw socket for that reason.
func TestServerErrorCodeProtocol(t *testing.T) {
	t.Parallel()

	dialer := net.Dialer{}
	conn, err := dialer.DialContext(t.Context(), "tcp", startServer(t))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// A command frame must be a RESP array; an inline line is not one.
	if _, err = conn.Write([]byte("GET users name\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	reply, err := protocol.ReadReply(bufio.NewReader(conn), 0)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if reply.Kind != protocol.ReplyError || reply.Code != client.CodeProtocol {
		t.Errorf("reply = %#v, want a %s error", reply, client.CodeProtocol)
	}
}

// TestErrorCodeSpellings locks the exported constants to the tokens that go on the wire. The matrix above proves the
// server reports each code; this proves the constants a caller compares against are those same strings, which is the
// half a refactor could silently change.
func TestErrorCodeSpellings(t *testing.T) {
	t.Parallel()

	for code, want := range map[string]string{
		client.CodeErr:            "ERR",
		client.CodeProtocol:       "PROTOCOL",
		client.CodeUnknownCommand: "UNKNOWNCMD",
		client.CodeArity:          "ARITY",
		client.CodeArgument:       "ARGUMENT",
		client.CodeTooLarge:       "TOOLARGE",
		client.CodeWrongType:      "WRONGTYPE",
		client.CodeReadOnly:       "READONLY",
		client.CodeUnavailable:    "UNAVAILABLE",
	} {
		if code != want {
			t.Errorf("code constant = %q, want %q", code, want)
		}
	}
}

// TestUnrecognizedCodeDegradesToErr pins the forward-compatibility rule: a later v1.x server may give a condition a
// narrower code, and a client built before that code existed must still report it as CodeErr — the code it reported
// for that condition before — keeping the token in the message.
func TestUnrecognizedCodeDegradesToErr(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		reply protocol.Reply
		code  string
		msg   string
	}{
		{name: "unknown code", reply: protocol.CodedError("FUTURE", "narrower failure"), code: client.CodeErr,
			msg: "FUTURE narrower failure"},
		{name: "known code", reply: protocol.CodedError(client.CodeWrongType, "key holds string"),
			code: client.CodeWrongType, msg: "key holds string"},
		{name: "plain ERR", reply: protocol.Error("unspecified"), code: client.CodeErr, msg: "unspecified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := client.NewWithTransport(&fakeTransport{resp: tc.reply})
			_, err := c.Get(t.Context(), "users", "name")
			serverErr, ok := errors.AsType[*client.ServerError](err)
			if !ok {
				t.Fatalf("Get() error = %v, want *ServerError", err)
			}
			if serverErr.Code != tc.code || serverErr.Msg != tc.msg {
				t.Errorf("ServerError = {%q, %q}, want {%q, %q}", serverErr.Code, serverErr.Msg, tc.code, tc.msg)
			}
		})
	}
}

// TestCloseIsIdempotentAndTerminal covers the lifecycle the Client type documents: closing twice is not an error, and a
// closed client fails later commands rather than dialling the still-running server again.
func TestCloseIsIdempotentAndTerminal(t *testing.T) {
	t.Parallel()

	addr := startServer(t)
	c, err := client.New(client.WithAddress(addr))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx := t.Context()
	if sErr := c.Set(ctx, "users", "name", "vlad"); sErr != nil {
		t.Fatalf("Set() error = %v", sErr)
	}

	if err = c.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err = c.Close(); err != nil {
		t.Errorf("second Close() error = %v, want nil", err)
	}
	if _, err = c.Get(ctx, "users", "name"); err == nil {
		t.Error("Get() after Close succeeded; a closed client must not reconnect")
	}
}

func mustClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.New(client.WithAddress(addr))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func seed(t *testing.T, c *client.Client, ctx context.Context, table, key, value string) {
	t.Helper()
	if err := c.Set(ctx, table, key, value); err != nil {
		t.Fatalf("Set(%s/%s) error = %v", table, key, err)
	}
}

// rawErr sends a command line expected to fail and returns the error.
func rawErr(t *testing.T, c *client.Client, ctx context.Context, command string) error {
	t.Helper()
	got, err := c.Raw(ctx, command)
	if err == nil {
		t.Fatalf("Raw(%s) = %q, want an error", command, got)
	}
	return err
}

func requireCode(t *testing.T, err error, want string) {
	t.Helper()
	var serverErr *client.ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("error = %v (%T), want *client.ServerError", err, err)
	}
	if serverErr.Code != want {
		t.Errorf("error code = %q, want %q (message: %s)", serverErr.Code, want, serverErr.Msg)
	}
}

// TestDurableServerRejectsOversizedRecord is the end-to-end half of the WAL record-size limit: a server with WAL
// persistence and a message-size limit above 64 MiB accepts the command off the wire, and the WAL then refuses to
// persist it. The client has to see TOOLARGE, the same code every other size limit reports.
//
// It moves ~64 MiB through a loopback socket, so it is skipped under -short.
func TestDurableServerRejectsOversizedRecord(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("moves ~64 MiB through the WAL append path")
	}

	// Both limits sit above the WAL's, so the frame is accepted and the WAL is what rejects it.
	const limitKB = 80 << 10

	writer, err := wal.OpenWriter(wal.WriterConfig{
		Dir:         t.TempDir(),
		Sync:        wal.SyncNo,
		SegmentSize: 1 << 20,
	}, 0)
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	addr, _ := startServerWithStorage(t,
		storage.New(engine.New(), storage.WithWAL(writer)),
		network.WithServerMaxMessageSize(limitKB*1024))

	c, err := client.New(client.WithAddress(addr), client.WithMaxMessageSize(limitKB))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	requireCode(t, c.Set(t.Context(), "users", "big", strings.Repeat("a", wal.MaxRecordSize)), client.CodeTooLarge)

	// The rejection is durable-side, so nothing was logged and a following write still works.
	if sErr := c.Set(t.Context(), "users", "small", "vlad"); sErr != nil {
		t.Fatalf("Set after a rejected oversized write: %v", sErr)
	}
	if writer.LastLSN() != 1 {
		t.Errorf("LastLSN = %d, want 1 (only the small write logged)", writer.LastLSN())
	}
}

// TestMessageSizeLimits pins how max_message_size bounds each direction, as COMPATIBILITY.md documents it. A server
// refuses a request past its limit, and refuses a TABLES or KEYS listing past it too — as a coded TOOLARGE, with the
// connection intact, rather than writing a reply a client with the same limit could not read. Other replies are not
// bounded by the server: a value grown past the limit by APPEND is written in full, and what protects a client there is
// its own limit, which fails locally as a transport error.
func TestMessageSizeLimits(t *testing.T) {
	t.Parallel()

	const serverLimit = 256

	addr, _ := startServerWithStorage(t, storage.New(engine.New(), storage.WithListLimit(serverLimit)),
		network.WithServerMaxMessageSize(serverLimit))

	// A generous client limit, so what follows measures the server's behavior rather than the client's.
	c := mustClientWithLimit(t, addr, 64)
	ctx := t.Context()
	for i := range 60 {
		seed(t, c, ctx, "t", fmt.Sprintf("key-%020d", i), "v")
	}

	// A request past the server's limit is refused with TOOLARGE.
	requireCode(t, c.Set(ctx, "t", "big", strings.Repeat("a", serverLimit)), client.CodeTooLarge)

	// So is a listing past it — refused whole, not truncated — and the same connection keeps working.
	_, err := c.Keys(ctx, "t")
	requireCode(t, err, client.CodeTooLarge)
	if tables, tErr := c.Tables(ctx); tErr != nil || !slices.Equal(tables, []string{"t"}) {
		t.Fatalf("Tables() after a refused listing = %v, %v; want [t]", tables, tErr)
	}

	// Any other reply is written in full even past the server's limit: grow an array one small APPEND at a time.
	for i := range 60 {
		if _, aErr := c.Append(ctx, "t", "log", fmt.Sprintf("entry-%020d", i)); aErr != nil {
			t.Fatalf("Append() error = %v", aErr)
		}
	}
	value, err := c.Get(ctx, "t", "log")
	if err != nil {
		t.Fatalf("Get() of a value past the server limit: %v", err)
	}
	if len(value) <= serverLimit {
		t.Fatalf("the value (%d bytes) did not exceed the server limit (%d), so it proves nothing", len(value), serverLimit)
	}

	// There a client is protected by its own limit. Refusing to decode is a transport error rather than a coded server
	// error — the command itself was valid.
	small := mustClientWithLimit(t, addr, 1)
	_, err = small.Get(ctx, "t", "log")
	if err == nil {
		t.Fatal("Get() under a 1KB client limit succeeded, want a size error")
	}
	if serverErr, ok := errors.AsType[*client.ServerError](err); ok {
		t.Errorf("over-limit reply surfaced as *ServerError (code %q); it is a local transport error", serverErr.Code)
	}

	// The connection recovers: the client drops the socket and redials on the next call.
	if _, err = small.Get(ctx, "t", "key-"+strings.Repeat("0", 20)); err != nil {
		t.Errorf("Get() after an over-limit reply = %v, want the connection to have recovered", err)
	}
}

// TestHealthCommands covers PING and STATUS through the public client: both answer on a direct connection, STATUS as
// its frozen field list, and a pool refuses STATUS because it cannot say which server would be describing itself.
func TestHealthCommands(t *testing.T) {
	t.Parallel()

	addr := startServer(t)
	c := mustClient(t, addr)
	ctx := t.Context()

	if reply, err := c.Raw(ctx, "PING"); err != nil || reply != "PONG" {
		t.Errorf("Raw(PING) = %q, %v; want PONG", reply, err)
	}
	requireCode(t, rawErr(t, c, ctx, "PING extra"), client.CodeArity)

	reply, err := c.Raw(ctx, "STATUS")
	if err != nil {
		t.Fatalf("Raw(STATUS) error = %v", err)
	}
	lines := strings.Split(reply, "\n")
	if len(lines) != 2*len(status.Fields) {
		t.Fatalf("STATUS has %d lines, want %d", len(lines), 2*len(status.Fields))
	}
	for i, field := range status.Fields {
		if lines[2*i] != field {
			t.Errorf("STATUS field %d = %q, want %q", i, lines[2*i], field)
		}
	}
	if lines[1] != "true" {
		t.Errorf("STATUS ready = %q, want true", lines[1])
	}

	pooled, err := client.New(client.WithServers(client.Server{Address: addr, Role: client.RoleMaster}))
	if err != nil {
		t.Fatalf("New(pool) error = %v", err)
	}
	t.Cleanup(func() { _ = pooled.Close() })
	if reply, err = pooled.Raw(ctx, "PING"); err != nil || reply != "PONG" {
		t.Errorf("pooled Raw(PING) = %q, %v; want PONG", reply, err)
	}
	if _, err = pooled.Raw(ctx, "STATUS"); err == nil || !strings.Contains(err.Error(), "cannot be sent through a pool") {
		t.Errorf("pooled Raw(STATUS) error = %v, want the pool to refuse it", err)
	}
}

func mustClientWithLimit(t *testing.T, addr string, kb int) *client.Client {
	t.Helper()
	c, err := client.New(client.WithAddress(addr), client.WithMaxMessageSize(kb))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
