//go:generate mockgen -destination=mocks/mock_engine.go -package=mocks . Engine

package main

import (
	"context"
	"errors"
	"sync"
)

// Engine defines the interface for the storage engine.
type Engine interface {
	// Set sets the value for a given key.
	Set(ctx context.Context, key, value string) error
	// Get retrieves the value for a given key.
	Get(ctx context.Context, key string) (string, error)
	// Del deletes the value for a given key.
	Del(ctx context.Context, key string) error
}

type engine struct {
	store map[string]string
	mu    sync.RWMutex
}

// NewEngine creates a new Engine instance.
func NewEngine() Engine {
	return &engine{store: make(map[string]string)}
}

// Set sets the value for a given key.
func (e *engine) Set(ctx context.Context, key, value string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.store[key] = value
	return nil
}

// Get retrieves the value for a given key.
func (e *engine) Get(ctx context.Context, key string) (string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	val, ok := e.store[key]
	if !ok {
		return "", errors.New("not found")
	}
	return val, nil
}

// Del deletes the value for a given key.
func (e *engine) Del(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.store[key]; !ok {
		return errors.New("not found")
	}
	delete(e.store, key)
	return nil
}
