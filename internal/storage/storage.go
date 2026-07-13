package storage

import (
	"context"
	"errors"

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
	if err := s.engine.Set(ctx, args[0], args[1], args[2]); err != nil {
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
	if err := s.engine.Del(ctx, args[0], args[1]); err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			return protocol.Reply{}, ErrNotFound
		}
		return protocol.Reply{}, err
	}
	return protocol.SimpleString("OK"), nil
}

func fmtBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
