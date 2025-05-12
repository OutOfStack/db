//go:generate mockgen -source storage.go -destination=mocks/storage.go -package=storage_mocks

package storage

import (
	"context"
	"errors"
)

// Engine is an interface for a storage engine
type Engine interface {
	Set(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, key string) error
}

// Storage implements a storage layer that provides a simple key-value store
type Storage struct {
	engine Engine
}

// Storage is an interface for a storage layer that provides a simple key-value store
func New(engine Engine) *Storage {
	return &Storage{engine: engine}
}

// Execute executes the given command with arguments and returns the result or an error
func (s *Storage) Execute(ctx context.Context, cmd string, args []string) (string, error) {
	if len(args) == 0 {
		return "", errors.New("not enough args")
	}

	key := args[0]

	switch cmd {
	case "SET":
		if len(args) < 2 {
			return "", errors.New("not enough args")
		}

		value := args[1]

		if err := s.engine.Set(ctx, key, value); err != nil {
			return "", err
		}

		return "OK", nil
	case "GET":
		val, err := s.engine.Get(ctx, key)
		if err != nil {
			return "", err
		}

		return val, nil
	case "DEL":
		if err := s.engine.Del(ctx, key); err != nil {
			return "", err
		}

		return "OK", nil
	default:
		return "", nil
	}
}
