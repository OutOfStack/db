# Plan 02 — Protocol Framing & Argument Encoding

## Description

Replace the current "one TCP read = one whitespace-delimited text command" protocol with a
length-prefixed binary framing and a structured argument encoding. This fixes existing
correctness problems (a single `conn.Read` is not guaranteed to return one whole message; values
cannot contain spaces or newlines; requests larger than the buffer are dropped —
`internal/network/server.go`, `handleConnection`) and, critically, defines the record encoding
that the WAL (plan 03), the replication stream (plan 04), and typed values (plan 06) will reuse.

## Relationship to other plans

- **Independent of plan 01 (Tables).** The two touch different layers (wire format vs. command
  semantics) and can be built in either order or in parallel. Doing 01 first is still preferred
  so the new encoding's tests are written against the final 3-argument command shape.

## Design decisions

- **Frame format (transport level):** `uint32` big-endian payload length, then payload.
  Max payload enforced from `max_message_size` config (`internal/config/server.go`) — reject
  oversized frames with an error response instead of silently breaking the connection.
- **Payload format (message level):** RESP-inspired but minimal:
  - Request: `uint16` argument count, then per argument `uint32` length + raw bytes.
    First argument is the command name (`SET`, `GET`, `DEL`).
  - Response: 1 status byte (`+` ok / `-` error), then `uint32` length + payload bytes.
  - This makes arguments 8-bit-clean: values may contain spaces, newlines, or arbitrary bytes,
    and plan 06 can serialize typed values into an argument without escaping.
- **Encoding lives in a new package** `internal/protocol/` with `EncodeRequest/DecodeRequest`,
  `EncodeResponse/DecodeResponse`, and `ReadFrame/WriteFrame` helpers operating on `io.Reader`
  / `io.Writer`. Both server and client import it; plans 03/04 reuse the same primitives for
  log records.
- **No protocol version negotiation** in this iteration — server and client ship together.
  Reserve the first byte of the payload as a version/type tag so plan 04 can add
  server-to-server message types without a breaking change.

## Implementation steps

1. **New package** `internal/protocol/` with encode/decode + frame read/write and exhaustive
   unit tests (truncated frames, oversized frames, zero args, binary-unsafe bytes).
2. **Server** (`internal/network/server.go`)
   - Replace the raw `conn.Read(buf)` loop with `protocol.ReadFrame` using a `bufio.Reader`;
     handles partial reads and pipelined requests correctly.
   - `RequestHandler` signature changes from `func(ctx, []byte) []byte` to
     `func(ctx, cmd string, args []string) (string, error)` so the encoding stays inside the
     network layer.
3. **Client** (`internal/network/client.go`) — mirror change: `Send(cmd, args)` encodes a frame,
   reads exactly one framed response.
4. **Compute / Parser** (`internal/compute/compute.go`, `internal/parser/parser.go`)
   - The wire now carries pre-split arguments, so `Parser.Parse` no longer tokenizes; it becomes
     command validation (name + arity). Keep the interface shape so mocks change minimally.
5. **CLI** (`cmd/db-cli/main.go`, `internal/client/client.go`)
   - The CLI keeps its human-friendly text input; it tokenizes the line (now allowing quoted
     strings for values with spaces) and sends structured args.
6. **Pool** (`internal/pool/client.go`) — `Send(data []byte)` becomes `Send(cmd, args)`
   passing through to the new client API; retry/failover logic unchanged.

## Testing

- Protocol round-trip and fuzz tests (`go test -fuzz`) on decode.
- Integration test: client sends value containing spaces/newlines/null bytes; large-but-legal
  request near the size limit; request over the limit gets a clean error.
- `make test` with race detection; verify with two concurrent CLI clients.

## Out of scope

- Compression, TLS, authentication.
- Backward compatibility with the old plain-text protocol (netcat debugging is lost; the CLI
  replaces it).
