//go:generate mockgen -destination=mocks/mock_storage.go -package=mocks . StorageLayer

package main

import "context"

// StorageLayer defines the interface for the storage layer, which executes commands.
type StorageLayer interface {
	// Execute executes the given command with arguments and returns the result or an error.
	Execute(ctx context.Context, cmd string, args []string) (string, error)
}

type storageLayer struct {
	engine Engine
}

// NewStorageLayer creates a new StorageLayer with the given engine.
func NewStorageLayer(engine Engine) StorageLayer {
	return &storageLayer{engine: engine}
}

// Execute executes the given command with arguments and returns the result or an error.
func (s *storageLayer) Execute(ctx context.Context, cmd string, args []string) (string, error) {
	switch cmd {
	case "SET":
		return "OK", s.engine.Set(ctx, args[0], args[1])
	case "GET":
		return s.engine.Get(ctx, args[0])
	case "DEL":
		return "OK", s.engine.Del(ctx, args[0])
	default:
		return "", nil
	}
}
