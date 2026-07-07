package storage

import (
	"context"
	"errors"

	"github.com/OutOfStack/db/internal/engine"
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
}

// Storage implements a storage layer that provides a simple key-value store
type Storage struct {
	engine Engine
}

// New returns a new Storage instance
func New(engine Engine) *Storage {
	return &Storage{engine: engine}
}

// Execute executes the given command with arguments and returns the result or an error
func (s *Storage) Execute(ctx context.Context, cmd string, args []string) (string, error) {
	if len(args) < 2 {
		return "", errors.New("not enough args")
	}

	table, key := args[0], args[1]

	switch cmd {
	case "SET":
		if len(args) < 3 {
			return "", errors.New("not enough args")
		}

		value := args[2]

		if err := s.engine.Set(ctx, table, key, value); err != nil {
			return "", err
		}

		return "OK", nil
	case "GET":
		val, err := s.engine.Get(ctx, table, key)
		if err != nil {
			if errors.Is(err, engine.ErrNotFound) {
				return "", ErrNotFound
			}
			return "", err
		}

		return val, nil
	case "DEL":
		if err := s.engine.Del(ctx, table, key); err != nil {
			if errors.Is(err, engine.ErrNotFound) {
				return "", ErrNotFound
			}
			return "", err
		}

		return "OK", nil
	default:
		return "", nil
	}
}
