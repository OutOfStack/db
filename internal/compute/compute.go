package compute

import (
	"context"
	"errors"
	"log/slog"
	"strings"

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

// Admin handles replication control-plane commands that live above the storage
// layer and are never written to the WAL.
type Admin interface {
	Promote(ctx context.Context) (protocol.Reply, error)
	Status(ctx context.Context) (protocol.Reply, error)
}

// Compute represents compute layer
type Compute struct {
	parser  Parser
	storage Storage
	admin   Admin
	logger  *slog.Logger
}

// Option configures a Compute.
type Option func(*Compute)

// WithAdmin wires a replication admin handler for PROMOTE and REPLICATION STATUS.
func WithAdmin(admin Admin) Option {
	return func(c *Compute) { c.admin = admin }
}

// New creates a new Compute with the given parser, storage, and logger
func New(parser Parser, storage Storage, logger *slog.Logger, options ...Option) *Compute {
	c := &Compute{parser: parser, storage: storage, logger: logger}
	for _, option := range options {
		option(c)
	}
	return c
}

// HandleRequest validates and executes a decoded request.
func (c *Compute) HandleRequest(ctx context.Context, cmd string, args []string) (protocol.Reply, error) {
	cmd, args, err := c.parser.Parse(cmd, args)
	if err != nil {
		c.logger.Error("Parse error", "error", err)
		return protocol.Reply{}, err
	}

	c.logger.Info("Parsed command", "cmd", cmd, "args", args)

	if reply, handled, adminErr := c.handleAdmin(ctx, cmd, args); handled {
		return reply, adminErr
	}

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

// handleAdmin dispatches replication control commands. handled is true when cmd
// is such a command, in which case the caller returns reply/err directly.
func (c *Compute) handleAdmin(ctx context.Context, cmd string, args []string) (protocol.Reply, bool, error) {
	switch cmd {
	case "PROMOTE":
		if c.admin == nil {
			return protocol.Reply{}, true, errors.New("replication not enabled")
		}
		reply, err := c.admin.Promote(ctx)
		return reply, true, err
	case "REPLICATION":
		if len(args) != 1 || !strings.EqualFold(args[0], "STATUS") {
			return protocol.Reply{}, true, errors.New("usage: REPLICATION STATUS")
		}
		if c.admin == nil {
			return protocol.Reply{}, true, errors.New("replication not enabled")
		}
		reply, err := c.admin.Status(ctx)
		return reply, true, err
	default:
		return protocol.Reply{}, false, nil
	}
}
