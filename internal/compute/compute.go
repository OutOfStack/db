package compute

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
)

const (
	commandPromote     = "PROMOTE"
	commandReplication = "REPLICATION"
)

// Storage is an interface for a storage layer
type Storage interface {
	Execute(ctx context.Context, cmd string, args []string) (protocol.Reply, error)
}

// Parser is an interface for a parser
type Parser interface {
	Parse(cmd string, args []string) (string, []string, error)
}

// Admin handles replication control-plane commands that live above the storage layer and are never written to the WAL.
type Admin interface {
	Promote(ctx context.Context) (protocol.Reply, error)
	Status(ctx context.Context) (protocol.Reply, error)
}

// Compute represents compute layer
type Compute struct {
	parser         Parser
	storage        Storage
	admin          Admin
	promoteEnabled bool
	logger         *slog.Logger
}

// Option configures a Compute.
type Option func(*Compute)

// WithAdmin wires a replication admin handler for PROMOTE and REPLICATION STATUS.
func WithAdmin(admin Admin) Option {
	return func(c *Compute) { c.admin = admin }
}

// WithPromoteEnabled permits PROMOTE when enabled is true. Off by default: promotion changes which node accepts
// writes, so it has to be an explicit operator decision (replication.allow_remote_promote in the server config).
func WithPromoteEnabled(enabled bool) Option {
	return func(c *Compute) { c.promoteEnabled = enabled }
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
func (c *Compute) HandleRequest(ctx context.Context, cmd string, args []string) (reply protocol.Reply, err error) {
	started := time.Now()
	c.logger.Debug("Received command", "cmd", cmd, "args", args)
	cmd, args, err = c.parser.Parse(cmd, args)
	if err != nil {
		// Rejected command names and error text may contain arbitrary user data.
		c.logger.Info("Command completed", "cmd", "invalid", "outcome", "parse_error", "duration", time.Since(started))
		c.logger.Debug("Parse error details", "error", err)
		return protocol.Reply{}, err
	}
	defer func() { c.logOutcome(cmd, args, started, reply, err) }()

	if adminReply, handled, adminErr := c.handleAdmin(ctx, cmd, args); handled {
		return adminReply, adminErr
	}

	reply, err = c.storage.Execute(ctx, cmd, args)
	if err != nil {
		return protocol.Reply{}, err
	}
	return reply, nil
}

func (c *Compute) logOutcome(cmd string, args []string, started time.Time, reply protocol.Reply, err error) {
	outcome := "ok"
	switch {
	case errors.Is(err, storage.ErrNotFound):
		outcome = "not_found"
	case err != nil || reply.Kind == protocol.ReplyError:
		outcome = "error"
	}
	attrs := []any{"cmd", cmd, "outcome", outcome, "duration", time.Since(started)}
	if cmd != commandPromote && cmd != commandReplication {
		if len(args) > 0 {
			attrs = append(attrs, "table_bytes", len(args[0]))
		}
		if len(args) > 1 {
			attrs = append(attrs, "key_bytes", len(args[1]))
		}
	}
	c.logger.Info("Command completed", attrs...)
	if err != nil {
		c.logger.Debug("Command error details", "error", err)
	}
}

// handleAdmin dispatches replication control commands. handled is true when cmd is such a command, in which case the
// caller returns reply/err directly.
func (c *Compute) handleAdmin(ctx context.Context, cmd string, args []string) (protocol.Reply, bool, error) {
	switch cmd {
	case commandPromote:
		if c.admin == nil {
			return protocol.Reply{}, true, errors.New("replication not enabled")
		}
		if !c.promoteEnabled {
			return protocol.Reply{}, true, errors.New("PROMOTE is disabled; set replication.allow_remote_promote to enable it")
		}
		reply, err := c.admin.Promote(ctx)
		return reply, true, err
	case commandReplication:
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
