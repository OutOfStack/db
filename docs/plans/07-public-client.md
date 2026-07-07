# Plan 07 — Public Client Package (decouple `cmd/db-cli` from `internal/`)

## Description

Today `cmd/db-cli/main.go` reaches directly into `internal/client` and `internal/config`, and
`internal/client` wires together `internal/network` + `internal/pool`. That works only because
the CLI lives in the same module — Go's `internal/` rule makes the client library unimportable
by any external program, so the database currently has no usable Go driver.

This plan extracts a public, importable client package at `github.com/OutOfStack/db/client`
(top-level `client/` directory) with a stable, typed API. The CLI becomes a thin REPL over that
package, and external users get the same entry point:

```go
import "github.com/OutOfStack/db/client"

c, err := client.New(client.WithAddress("127.0.0.1:3223"))
err = c.Set(ctx, "users", "name", "vlad")
val, err := c.Get(ctx, "users", "name")
err = c.Del(ctx, "users", "name")
```

## Relationship to other plans

- **Depends on plan 01 (Tables), already implemented:** the typed API embeds the
  `(table, key)` shape in method signatures.
- **Best implemented immediately before plan 02 (Protocol framing).** Plan 02 rewrites the wire
  format and the transport `Send` signature — a breaking change. If the public API lands first,
  that break is absorbed *inside* the client package: `Set/Get/Del` signatures stay identical
  while their implementation switches from raw text to framed protocol. Doing 07 after 02 works
  too, but then the CLI must be rewired twice.
- **Feeds plan 04 (Replication):** read/write routing (writes → master, reads → any) becomes an
  internal concern of this package; users of `Set`/`Get` never see it.
- **Affected by plan 06 (Typed values):** `Get` returning `string` will need a typed variant.
  Design for it now (see API notes) rather than breaking the API later.
- **Independent of plans 03 and 05** (server-side persistence/tiering are invisible to clients).

## Design decisions

- **Location: top-level `client/` package**, not `pkg/client`. The import path
  `github.com/OutOfStack/db/client` reads naturally, and a single public package doesn't need a
  `pkg/` umbrella. Everything else stays under `internal/` — the public surface is exactly one
  package.
- **Configuration via functional options, not config structs.** The package must not depend on
  `internal/config` or YAML. Options mirror what exists today:
  - `WithAddress(addr string)` — single-server mode
  - `WithServers(servers ...Server)` where `Server{Address string; Role Role}` — pool mode
  - `WithStrategy(s Strategy)` (`MasterFirst`, `RoundRobin`, `Random`)
  - `WithRetries(n int, delay time.Duration)`, `WithFailureTimeout(d time.Duration)`
  - `WithIdleTimeout(d time.Duration)`, `WithMaxMessageSize(kb int)`
  This means re-declaring small public types (`Server`, `Role`, `Strategy`) in `client` and
  converting to `internal/pool` types inside — deliberate duplication so internal refactors
  (plans 02/04) never leak into the public API.
- **Typed methods + an escape hatch.**
  - `Set(ctx, table, key, value string) error`
  - `Get(ctx, table, key string) (string, error)` — returns `client.ErrNotFound` (sentinel) when
    the key is missing, instead of making callers string-match the "not found" response
  - `Del(ctx, table, key string) error`
  - `Raw(ctx, command string) (string, error)` — passes a raw command line through; keeps the
    CLI a dumb REPL and gives users access to future commands before typed wrappers exist
  - `Close() error`
  - Plan 06 extension path: add `GetValue(ctx, table, key) (Value, error)` later; `Get` remains
    as the string convenience — no breaking change.
- **`context.Context` in every method signature from day one**, even though the current
  transport can't honor cancellation mid-call. Plan 02's rewrite plugs deadlines in
  (`SetReadDeadline` from `ctx`); the API doesn't change when it does. Retrofitting ctx into a
  published API later is far more painful than a temporarily-ignored parameter.
- **Error contract:** `ErrNotFound` sentinel; other server errors surface as
  `&client.ServerError{Msg string}`. Until plan 02 adds a status byte to responses, detection is
  string-based inside the package (one place to fix, invisible to callers).
- **`internal/client` is deleted**, absorbed by the new package. `internal/network` and
  `internal/pool` remain internal; `client` is the only package allowed to import them from the
  public side (same module, so `internal/` imports are legal for it).
- **CLI ownership split:** flag parsing and YAML config loading stay in `cmd/db-cli` (they are
  CLI concerns, using `internal/config` is fine there since it ships in this module); the CLI
  maps loaded config → `client` options, then loops `client.Raw()` over stdin lines. The
  `network.Client` interface used by the CLI today disappears from its code.

## Implementation steps

1. **Create `client/` package:** `Client`, `Option` funcs, `Server`/`Role`/`Strategy` types,
   `ErrNotFound`, `ServerError`. Internally: build `pool.PoolConfig` or single
   `network.TCPClient` exactly as `internal/client.New` does now.
2. **Implement `Set`/`Get`/`Del`** as command formatting + response interpretation over the
   existing text protocol ("OK", value, "not found" mapping to `ErrNotFound`). Implement `Raw`
   as the passthrough.
3. **Rewrite `cmd/db-cli/main.go`:** keep flags + `internal/config` loading, translate
   `config.ClientConfig` → `[]client.Option`, REPL loop over `client.Raw`.
4. **Delete `internal/client`;** grep for remaining importers (only the CLI today).
5. **Tests:** unit tests for option validation and error mapping (mock transport via a small
   internal interface); integration test starting a real server and exercising
   `Set`/`Get`/`Del`/`ErrNotFound` round-trip. This becomes the seed of the driver test suite
   plans 02/04 will extend.
6. **Docs:** README gains a "Go client library" section with the import example; note that
   `client` is the only supported import path for external consumers.

## Testing

- `go test ./client/...` — options, error mapping, typed methods against a mock transport.
- Integration: in-process server on an ephemeral port; verify `ErrNotFound` on missing key,
  pool failover path still works through the public API (reuse existing pool tests' approach).
- Verify module boundary: a scratch module importing `github.com/OutOfStack/db/client` compiles
  (this is the whole point of the plan — worth one explicit check).

## Out of scope

- Publishing/tagging a semver release (`v0.x` tags) — worth doing once plan 02 stabilizes the
  protocol, since every tagged version freezes the API.
- Connection pooling redesign, TLS, auth — the package wraps what exists.
- Typed-value API (`GetValue`) — arrives with plan 06; only the extension point is reserved here.
