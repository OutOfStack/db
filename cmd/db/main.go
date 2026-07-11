package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/network"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.Parse()

	cfg, err := config.LoadServerConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v\n", err)
	}

	var logger *slog.Logger
	level := slog.LevelInfo
	switch cfg.Logging.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: level}
	logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	if cfg.Logging.Output != "" {
		file, fErr := os.OpenFile(cfg.Logging.Output, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if fErr != nil {
			logger.Error("Failed to open log file", "error", fErr)
			os.Exit(1)
		}
		defer func() {
			if fErr = file.Close(); fErr != nil {
				logger.Error("Failed to close log file", "error", fErr)
			}
		}()
		logger = slog.New(slog.NewJSONHandler(file, opts))
	}

	store := storage.New(engine.New())
	comp := compute.New(parser.New(), store, logger)

	srv, err := network.NewTCPServer(cfg.Network.Address, logger,
		network.WithServerIdleTimeout(cfg.Network.IdleTimeout),
		network.WithServerMaxMessageSize(cfg.Network.MaxMessageSizeKB*1024),
		network.WithServerMaxConnections(cfg.Network.MaxConnections))
	if err != nil {
		logger.Error("Failed to create server", "error", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Server started", "address", cfg.Network.Address)

	go srv.Start(ctx, func(ctx context.Context, cmd string, args []string) protocol.Reply {
		res, rErr := comp.HandleRequest(ctx, cmd, args)
		if rErr != nil {
			if errors.Is(rErr, storage.ErrNotFound) {
				return protocol.NullBulkString()
			}
			return protocol.Error(rErr.Error())
		}
		switch strings.ToUpper(cmd) {
		case "GET":
			return protocol.BulkString(res)
		case "SET", "DEL":
			return protocol.SimpleString(res)
		default:
			return protocol.BulkString(res)
		}
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down server...")
}
