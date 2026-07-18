package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
)

var (
	// ErrNotFound is the error returned when a key is not found
	ErrNotFound = errors.New("not found")
)

// Engine is an interface for a storage engine
type Engine interface {
	Set(ctx context.Context, table, key, value string) error
	Get(ctx context.Context, table, key string) (string, error)
	Del(ctx context.Context, table, key string) error
	Tables(ctx context.Context) []string
	TableExists(ctx context.Context, table string) bool
	Keys(ctx context.Context, table string) []string
	Range(fn func(table, key, value string) bool)
}

// WAL is the persistence stream used for mutating commands.
type WAL interface {
	Append(ctx context.Context, command string, args []string) (uint64, error)
	LastLSN() uint64
	Prune(ctx context.Context, uptoLSN uint64) error
}

// SnapshotSource is the read-only state exposed to snapshot writers.
type SnapshotSource interface {
	Range(fn func(table, key, value string) bool)
}

// Option configures Storage.
type Option func(*Storage)

// WithWAL enables write-ahead logging for mutations.
func WithWAL(log WAL) Option {
	return func(storage *Storage) { storage.wal = log }
}

// Storage implements a storage layer that provides a simple key-value store
type Storage struct {
	engine Engine
	wal    WAL
	// mu serializes snapshots (write lock) against mutations (read lock); many
	// mutations may run concurrently so their WAL appends can be group-committed.
	mu   sync.RWMutex
	gate *applyGate
}

// New returns a new Storage instance
func New(engine Engine, options ...Option) *Storage {
	storage := &Storage{engine: engine}
	for _, option := range options {
		option(storage)
	}
	if storage.wal != nil {
		storage.gate = newApplyGate(storage.wal.LastLSN() + 1)
	}
	return storage
}

// applyGate applies engine mutations in strictly increasing LSN order. Concurrent
// mutations append to the WAL in parallel (enabling group commit) but must land in
// the engine in the same order the WAL assigned, so replay reproduces the state.
type applyGate struct {
	mu   sync.Mutex
	cond *sync.Cond
	next uint64
}

func newApplyGate(next uint64) *applyGate {
	gate := &applyGate{next: next}
	gate.cond = sync.NewCond(&gate.mu)
	return gate
}

// run waits until lsn is the next LSN to apply, runs fn, then releases the next
// LSN. The gate always advances: the WAL record is already durable, so a benign
// engine error (e.g. deleting a missing key) must not stall later mutations.
func (g *applyGate) run(lsn uint64, fn func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for lsn != g.next {
		g.cond.Wait()
	}
	err := fn()
	g.next++
	g.cond.Broadcast()
	return err
}

// Snapshot writes a state/LSN-consistent snapshot, then prunes incorporated WAL
// segments. Mutations are paused only long enough to copy the state and capture
// its LSN; the disk write and prune run without blocking mutations or reads.
func (s *Storage) Snapshot(
	ctx context.Context,
	write func(context.Context, uint64, SnapshotSource) error,
) error {
	if s.wal == nil {
		return nil
	}

	s.mu.Lock()
	lsn := s.wal.LastLSN()
	source := captureState(s.engine)
	s.mu.Unlock()

	if err := write(ctx, lsn, source); err != nil {
		return err
	}
	return s.wal.Prune(ctx, lsn)
}

// snapshotEntry is one captured value; captured is a point-in-time copy of the
// engine used so snapshot disk I/O happens without holding the mutation lock.
type snapshotEntry struct{ table, key, value string }

type captured []snapshotEntry

func (c captured) Range(fn func(table, key, value string) bool) {
	for _, entry := range c {
		if !fn(entry.table, entry.key, entry.value) {
			return
		}
	}
}

func captureState(source SnapshotSource) captured {
	var entries captured
	source.Range(func(table, key, value string) bool {
		entries = append(entries, snapshotEntry{table: table, key: key, value: value})
		return true
	})
	return entries
}

// Execute executes the given command with arguments and returns the result or an error
func (s *Storage) Execute(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	switch cmd {
	case "SET":
		return s.set(ctx, args)
	case "GET":
		return s.get(ctx, args)
	case "DEL":
		return s.del(ctx, args)
	case "TABLES":
		return protocol.BulkStringArray(s.engine.Tables(ctx)), nil
	case "EXISTS":
		return protocol.BulkString(fmtBool(s.engine.TableExists(ctx, args[0]))), nil
	case "KEYS":
		return protocol.BulkStringArray(s.engine.Keys(ctx, args[0])), nil
	default:
		return protocol.Reply{}, nil
	}
}

func (s *Storage) set(ctx context.Context, args []string) (protocol.Reply, error) {
	apply := func() error { return s.engine.Set(ctx, args[0], args[1], args[2]) }
	if err := s.mutate(ctx, "SET", args, apply); err != nil {
		return protocol.Reply{}, err
	}
	return protocol.SimpleString("OK"), nil
}

func (s *Storage) get(ctx context.Context, args []string) (protocol.Reply, error) {
	val, err := s.engine.Get(ctx, args[0], args[1])
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return protocol.Reply{}, ErrNotFound
		}
		return protocol.Reply{}, err
	}
	return protocol.BulkString(val), nil
}

func (s *Storage) del(ctx context.Context, args []string) (protocol.Reply, error) {
	apply := func() error { return s.engine.Del(ctx, args[0], args[1]) }
	if err := s.mutate(ctx, "DEL", args, apply); err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return protocol.Reply{}, ErrNotFound
		}
		return protocol.Reply{}, err
	}
	return protocol.SimpleString("OK"), nil
}

// mutate durably logs a mutation, then applies it to the engine. When the WAL is
// enabled, appends run concurrently (under the shared read lock) so the writer can
// group-commit them, while the apply gate replays them into the engine in LSN order.
func (s *Storage) mutate(ctx context.Context, command string, args []string, apply func() error) error {
	if s.wal == nil {
		return apply()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	lsn, err := s.wal.Append(ctx, command, args)
	if err != nil {
		return err
	}
	return s.gate.run(lsn, apply)
}

func fmtBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
