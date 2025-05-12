//go:generate mockgen -destination=mocks/mock_compute.go -package=mocks . ComputeLayer

package main

import (
	"context"
	"log/slog"
)

// ComputeLayer defines the interface for the compute layer, which processes requests and delegates to the storage layer.
type ComputeLayer interface {
	HandleRequest(ctx context.Context, input string) (string, error)
}

type computeLayer struct {
	parser  Parser
	storage StorageLayer
	logger  *slog.Logger
}

// NewComputeLayer creates a new ComputeLayer with the given parser, storage, and logger.
func NewComputeLayer(parser Parser, storage StorageLayer, logger *slog.Logger) ComputeLayer {
	return &computeLayer{parser: parser, storage: storage, logger: logger}
}

// HandleRequest processes the input request string and returns the result or an error.
func (c *computeLayer) HandleRequest(ctx context.Context, input string) (string, error) {
	cmd, args, err := c.parser.Parse(input)
	if err != nil {
		c.logger.Error("Parse error", "err", err)
		return "", err
	}
	c.logger.Info("Parsed command", "cmd", cmd, "args", args)
	result, err := c.storage.Execute(ctx, cmd, args)
	if err != nil {
		c.logger.Error("Storage execution error", "err", err)
	}
	return result, err
}
