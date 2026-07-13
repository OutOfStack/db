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
