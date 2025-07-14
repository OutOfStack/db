package compute

import (
	"context"
	"log/slog"
)

// Storage is an interface for a storage layer
type Storage interface {
	Execute(ctx context.Context, cmd string, args []string) (string, error)
}

// Parser is an interface for a parser
type Parser interface {
	Parse(input string) (string, []string, error)
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

// HandleRequest processes the input request string and returns the result or an error
func (c *Compute) HandleRequest(ctx context.Context, input string) (string, error) {
	cmd, args, err := c.parser.Parse(input)
	if err != nil {
		c.logger.Error("Parse error", "error", err)
		return "", err
	}

	c.logger.Info("Parsed command", "cmd", cmd, "args", args)

	result, err := c.storage.Execute(ctx, cmd, args)
	if err != nil {
		c.logger.Error("Storage execution error", "error", err)
		return "", err
	}

	return result, nil
}
