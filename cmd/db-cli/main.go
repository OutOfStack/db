package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/config"
)

func main() {
	var configPath, address string
	var timeout time.Duration
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.StringVar(&address, "address", "", "Database server address (overrides config)")
	flag.DurationVar(&timeout, "timeout", 0, "Connection idle timeout (overrides config)")
	flag.Parse()

	cfg, err := config.LoadClientConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v\n", err)
	}

	// apply flag overrides
	if address != "" {
		cfg.Network.Address = address
	}
	if timeout > 0 {
		cfg.Network.IdleTimeout = timeout
	}

	dbClient, err := client.New(clientOptions(cfg)...)
	if err != nil {
		fmt.Printf("Failed to connect to database server: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err = dbClient.Close(); err != nil {
			fmt.Printf("Failed to close connection: %v\n", err)
		}
	}()

	if cfg.Pool.Enabled {
		fmt.Printf("Connected to database pool (%d servers)\n", len(cfg.Pool.Servers))
	} else {
		fmt.Printf("Connected to database server at %s\n", cfg.Network.Address)
	}
	fmt.Println("Available commands:")
	fmt.Println("  SET table key value")
	fmt.Println("  SET table key \"value with spaces\"")
	fmt.Println("  GET table key")
	fmt.Println("  DEL table key")
	fmt.Println("  TABLES")
	fmt.Println("  EXISTS table")
	fmt.Println("  KEYS table")
	fmt.Println("Type 'exit' to quit")
	fmt.Println()

	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "exit" {
			break
		}

		if input == "" {
			continue
		}

		response, sErr := dbClient.Raw(ctx, input)
		if sErr != nil {
			fmt.Printf("Failed to send command: %v\n", sErr)
			break
		}
		fmt.Println(response)
	}

	if err = scanner.Err(); err != nil {
		fmt.Printf("Input error: %v\n", err)
	}
}

// clientOptions maps the loaded CLI configuration to client options
func clientOptions(cfg *config.ClientConfig) []client.Option {
	opts := []client.Option{
		client.WithIdleTimeout(cfg.Network.IdleTimeout),
		client.WithMaxMessageSize(cfg.Network.MaxMessageSizeKB),
	}

	if !cfg.Pool.Enabled {
		return append(opts, client.WithAddress(cfg.Network.Address))
	}

	servers := make([]client.Server, 0, len(cfg.Pool.Servers))
	for _, s := range cfg.Pool.Servers {
		servers = append(servers, client.Server{
			Address: s.Address,
			Role:    client.Role(s.Role),
		})
	}
	return append(opts,
		client.WithServers(servers...),
		client.WithStrategy(client.Strategy(cfg.Pool.SelectionStrategy)),
		client.WithRetries(cfg.Pool.MaxRetries, cfg.Pool.RetryDelay),
		client.WithFailureTimeout(cfg.Pool.FailureTimeout),
	)
}
