package engine

import (
	"context"
	"errors"
	"sort"
	"sync"
)

const (
	// TypeInMemory is the type for the in-memory engine
	TypeInMemory = "in_memory"
)

var (
	// ErrNotFound is the error returned when a key is not found
	ErrNotFound = errors.New("key not found")
)

// Engine is an in-memory key-value store with keys scoped by table
type Engine struct {
	store map[string]map[string]string
	mu    sync.RWMutex
}

// Entry is one value used by the recovery bulk-load path.
type Entry struct {
	Table string
	Key   string
	Value string
}

// Tables returns all table names in sorted order.
func (e *Engine) Tables(_ context.Context) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tables := make([]string, 0, len(e.store))
	for table := range e.store {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

// TableExists reports whether a table currently contains at least one key.
func (e *Engine) TableExists(_ context.Context, table string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	_, ok := e.store[table]
	return ok
}

// Keys returns all keys in table in sorted order.
func (e *Engine) Keys(_ context.Context, table string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t := e.store[table]
	keys := make([]string, 0, len(t))
	for key := range t {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// New creates a new Engine instance
func New() *Engine {
	return &Engine{
		store: make(map[string]map[string]string),
		mu:    sync.RWMutex{},
	}
}

// Range calls fn for every stored value while holding a read lock. Iteration
// stops when fn returns false.
func (e *Engine) Range(fn func(table, key, value string) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for table, values := range e.store {
		for key, value := range values {
			if !fn(table, key, value) {
				return
			}
		}
	}
}

// Reset removes all stored data. A standby calls it during snapshot resync,
// before loading the master's snapshot, since the snapshot fully replaces the
// standby's superseded state.
func (e *Engine) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.store = make(map[string]map[string]string)
}

// Load inserts a recovered set of entries without routing them through the WAL.
func (e *Engine) Load(_ context.Context, entries []Entry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.load(entries)
}

// Replace atomically swaps all stored data for entries under a single lock, so a
// concurrent reader never observes the empty intermediate state that separate
// Reset + Load calls would expose. Used by snapshot resync on a serving standby.
func (e *Engine) Replace(entries []Entry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.store = make(map[string]map[string]string)
	e.load(entries)
}

// load inserts entries into the current store. The caller holds e.mu.
func (e *Engine) load(entries []Entry) {
	for _, entry := range entries {
		table := e.store[entry.Table]
		if table == nil {
			table = make(map[string]string)
			e.store[entry.Table] = table
		}
		table[entry.Key] = entry.Value
	}
}

// Set sets the value for a given key in a table, creating the table if it does not exist
func (e *Engine) Set(_ context.Context, table, key, value string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	t, ok := e.store[table]
	if !ok {
		t = make(map[string]string)
		e.store[table] = t
	}
	t[key] = value

	return nil
}

// Get retrieves the value for a given key in a table
func (e *Engine) Get(_ context.Context, table, key string) (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, ok := e.store[table][key]
	if !ok {
		return "", ErrNotFound
	}

	return val, nil
}

// Del deletes the value for a given key in a table, removing the table when it becomes empty
func (e *Engine) Del(_ context.Context, table, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	t, ok := e.store[table]
	if !ok {
		return ErrNotFound
	}
	if _, ok = t[key]; !ok {
		return ErrNotFound
	}

	delete(t, key)
	if len(t) == 0 {
		delete(e.store, table)
	}

	return nil
}
