package compute

import (
	"context"
	"errors"
	"log/slog"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
)

// Storage is an interface for a storage layer
type Storage interface {
	Execute(ctx context.Context, cmd string, args []string) (protocol.Reply, error)
}

// Parser is an interface for a parser
type Parser interface {
	Parse(cmd string, args []string) (string, []string, error)
}

// Compute represents compute layer
type Compute struct {
	parser  Parser
	storage Storage
	logger  *slog.Logger
}

// New creates a new Compute with the given parser, storage, and logger
func New(parser Parser, storage Storage, logger *slog.Logger) *Compute {
	return &Compute{parser: parser, storage: storage, logger: logger}
}

// HandleRequest validates and executes a decoded request.
func (c *Compute) HandleRequest(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	cmd, args, err := c.parser.Parse(cmd, args)
	if err != nil {
		c.logger.Error("Parse error", "error", err)
		return protocol.Reply{}, err
	}

	c.logger.Info("Parsed command", "cmd", cmd, "args", args)

	result, err := c.storage.Execute(ctx, cmd, args)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.logger.Info("Key not found", "args", args)
		} else {
			c.logger.Error("Storage execution error", "error", err)
		}
		return protocol.Reply{}, err
	}

	return result, nil
}
