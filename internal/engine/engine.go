package engine

import (
	"context"
	"errors"
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

// Engine is an in-memory key-value store
type Engine struct {
	store map[string]string
	mu    sync.RWMutex
}

// New creates a new Engine instance
func New() *Engine {
	return &Engine{
		store: make(map[string]string),
		mu:    sync.RWMutex{},
	}
}

// Set sets the value for a given key
func (e *Engine) Set(_ context.Context, key, value string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.store[key] = value

	return nil
}

// Get retrieves the value for a given key
func (e *Engine) Get(_ context.Context, key string) (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	val, ok := e.store[key]
	if !ok {
		return "", ErrNotFound
	}

	return val, nil
}

// Del deletes the value for a given key
func (e *Engine) Del(_ context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.store[key]; !ok {
		return ErrNotFound
	}

	delete(e.store, key)

	return nil
}
