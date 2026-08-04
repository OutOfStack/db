package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

var (
	// ErrNotFound is the error returned when a key is not found
	ErrNotFound = errors.New("not found")
	// ErrReadOnly is returned for mutating commands on a replication standby. It maps to the "ERR readonly" wire reply so
	// a pool client can re-route the write to a master.
	ErrReadOnly = errors.New("readonly")
)

// Engine is an interface for a storage engine
type Engine interface {
	Set(ctx context.Context, table, key, value string) error
	Get(ctx context.Context, table, key string) (string, error)
	Del(ctx context.Context, table, key string) error
	// Update atomically replaces a value with the result of fn, which sees the current value and whether it exists. It is
	// the read-modify-write primitive behind INCR, APPEND and HSET.
	Update(ctx context.Context, table, key string, fn func(old string, exists bool) (string, error)) error
	Tables(ctx context.Context) []string
	TableExists(ctx context.Context, table string) bool
	Keys(ctx context.Context, table string) []string
	Range(fn func(table, key, value string) bool)
	// Replace atomically swaps all state for a resync snapshot on a standby.
	Replace(entries []engine.Entry)
}

// WAL is the persistence stream used for mutating commands.
type WAL interface {
	Append(ctx context.Context, command string, args []string) (uint64, error)
	// AppendRecord persists a record with a caller-assigned LSN (replication).
	AppendRecord(ctx context.Context, record wal.Record) error
	// Reset discards all segments and sets LastLSN to lsn (snapshot resync).
	Reset(ctx context.Context, lsn uint64) error
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

// WithReadOnly starts the storage in read-only mode, rejecting every mutating command with ErrReadOnly. Replication
// standbys use it; Promote lifts it.
func WithReadOnly(readOnly bool) Option {
	return func(storage *Storage) { storage.readOnly.Store(readOnly) }
}

// Storage implements a storage layer that provides a simple key-value store
type Storage struct {
	engine Engine
	wal    WAL
	// mu serializes snapshots (write lock) against mutations (read lock); many mutations may run concurrently so their WAL
	// appends can be group-committed. It also serializes Promote's gate swap against in-flight mutations.
	mu       sync.RWMutex
	gate     *applyGate
	readOnly atomic.Bool
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

// applyGate applies engine mutations in strictly increasing LSN order. Concurrent mutations append to the WAL in
// parallel (enabling group commit) but must land in the engine in the same order the WAL assigned, so replay reproduces
// the state.
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

// run waits until lsn is the next LSN to apply, runs fn, then releases the next LSN. The gate always advances: the WAL
// record is already durable, so a benign engine error (e.g. deleting a missing key) must not stall later mutations.
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

// Snapshot writes a state/LSN-consistent snapshot, then prunes incorporated WAL segments. Mutations are paused only
// long enough to copy the state and capture its LSN; the disk write and prune run without blocking mutations or reads.
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

// snapshotEntry is one captured value; captured is a point-in-time copy of the engine used so snapshot disk I/O happens
// without holding the mutation lock.
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
		return s.literalMutation(ctx, wal.CommandSet, args)
	case "GET":
		return s.get(ctx, args)
	case "DEL":
		return s.mutation(ctx, wal.CommandDel, args)
	case "INCR":
		return s.incr(ctx, args)
	case "APPEND":
		return s.literalMutation(ctx, wal.CommandAppend, args)
	case "HSET":
		return s.literalMutation(ctx, wal.CommandHSet, args)
	case "HGET":
		return s.hget(ctx, args)
	case "TYPE":
		return s.valueType(ctx, args)
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

// literalMutation logs a mutation whose last argument is a value literal (SET, APPEND, HSET), replacing the literal
// with its encoding.
func (s *Storage) literalMutation(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	last := len(args) - 1
	value, err := protocol.ParseLiteral(args[last])
	if err != nil {
		return protocol.Reply{}, err
	}
	return s.mutation(ctx, cmd, append(slices.Clone(args[:last]), protocol.Encode(value)))
}

func (s *Storage) get(ctx context.Context, args []string) (protocol.Reply, error) {
	stored, err := s.load(ctx, args[0], args[1])
	if err != nil {
		return protocol.Reply{}, err
	}
	return protocol.BulkString(protocol.Render(protocol.Decode(stored))), nil
}

func (s *Storage) incr(ctx context.Context, args []string) (protocol.Reply, error) {
	literal := "1"
	if len(args) == 3 {
		literal = args[2]
	}
	delta, err := protocol.ParseLiteral(literal)
	if err != nil {
		return protocol.Reply{}, err
	}
	if delta.Kind != protocol.KindInt && delta.Kind != protocol.KindFloat {
		return protocol.Reply{}, fmt.Errorf("INCR delta must be int or float, got %s", delta.Kind)
	}
	return s.mutation(ctx, wal.CommandIncr, []string{args[0], args[1], protocol.Encode(delta)})
}

func (s *Storage) hget(ctx context.Context, args []string) (protocol.Reply, error) {
	stored, err := s.load(ctx, args[0], args[1])
	if err != nil {
		return protocol.Reply{}, err
	}
	value := protocol.Decode(stored)
	if value.Kind != protocol.KindMap {
		return protocol.Reply{}, wrongType("HGET", value.Kind, protocol.KindMap.String())
	}
	field, ok := value.Map[args[2]]
	if !ok {
		return protocol.Reply{}, ErrNotFound
	}
	return protocol.BulkString(protocol.Render(field)), nil
}

func (s *Storage) valueType(ctx context.Context, args []string) (protocol.Reply, error) {
	stored, err := s.load(ctx, args[0], args[1])
	if err != nil {
		return protocol.Reply{}, err
	}
	return protocol.SimpleString(protocol.Decode(stored).Kind.String()), nil
}

func (s *Storage) load(ctx context.Context, table, key string) (string, error) {
	value, err := s.engine.Get(ctx, table, key)
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	return value, nil
}

// mutation logs encoded arguments before applying them to the engine.
func (s *Storage) mutation(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	var reply protocol.Reply
	err := s.mutate(ctx, cmd, args, func() error {
		var applyErr error
		reply, applyErr = Apply(ctx, s.engine, cmd, args)
		return applyErr
	})
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return protocol.Reply{}, ErrNotFound
		}
		return protocol.Reply{}, err
	}
	return reply, nil
}

// mutate durably logs a mutation, then applies it to the engine. When the WAL is enabled, appends run concurrently
// (under the shared read lock) so the writer can group-commit them, while the apply gate replays them into the engine
// in LSN order.
func (s *Storage) mutate(ctx context.Context, command string, args []string, apply func() error) error {
	if s.readOnly.Load() {
		return ErrReadOnly
	}
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

// ReadOnly reports whether mutating commands are currently rejected.
func (s *Storage) ReadOnly() bool { return s.readOnly.Load() }

// Promote lifts read-only mode so the storage accepts writes. It resets the apply gate to the WAL's current tail,
// because replication advanced LastLSN past the value captured when the storage was created. Client writes are still
// rejected while the gate is swapped, so no mutation observes a stale gate.
func (s *Storage) Promote() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		s.gate = newApplyGate(s.wal.LastLSN() + 1)
	}
	s.readOnly.Store(false)
}

// ApplyReplicated persists a record streamed from a master and applies it to the engine. It holds the shared read lock
// so a concurrent Snapshot cannot capture a state whose LSN is ahead of the engine (which would drop the record on
// recovery). It bypasses the apply gate: a single master stream is already ordered by LSN.
func (s *Storage) ApplyReplicated(ctx context.Context, record wal.Record) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.wal.AppendRecord(ctx, record); err != nil {
		return err
	}
	return ApplyReplay(ctx, s.engine, record.Command, record.Args)
}

// ResetToSnapshot replaces all state with a snapshot received during resync: it persists the snapshot, resets the WAL
// to lsn, and reloads the engine, all under the exclusive lock so it is atomic against Snapshot.
func (s *Storage) ResetToSnapshot(ctx context.Context, dir string, lsn uint64, entries []engine.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset the WAL before publishing the snapshot. If we crash between the two, recovery falls back to the previous
	// snapshot plus an empty WAL and re-syncs — safe. The reverse order would leave a high-LSN snapshot alongside old
	// low-LSN segments, so the reopened writer appends a non-contiguous tail that recovery refuses.
	if err := s.wal.Reset(ctx, lsn); err != nil {
		return err
	}
	if err := wal.WriteSnapshot(ctx, dir, lsn, entrySource(entries)); err != nil {
		return err
	}
	s.engine.Replace(entries)
	return nil
}

// entrySource adapts recovered entries to the wal.SnapshotSource interface.
type entrySource []engine.Entry

func (e entrySource) Range(fn func(table, key, value string) bool) {
	for _, entry := range e {
		if !fn(entry.Table, entry.Key, entry.Value) {
			return
		}
	}
}

func fmtBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
