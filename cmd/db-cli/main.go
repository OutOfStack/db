package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/version"
)

func main() {
	var configPath, address string
	var showVersion bool
	var timeout time.Duration
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.BoolVar(&showVersion, "version", false, "Print release, commit, protocol and storage format versions, then exit")
	flag.StringVar(&address, "address", "", "Database server address (overrides config)")
	flag.DurationVar(&timeout, "timeout", 0, "Connection idle timeout (overrides config)")
	flag.Parse()

	if showVersion {
		fmt.Print(version.Get())
		return
	}

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

	// the client connects on first use, so an unreachable server surfaces on the first command rather than here
	dbClient, err := client.New(clientOptions(cfg)...)
	if err != nil {
		fmt.Printf("Invalid client configuration: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err = dbClient.Close(); err != nil {
			fmt.Printf("Failed to close connection: %v\n", err)
		}
	}()

	if cfg.Pool.Enabled {
		fmt.Printf("Using database pool (%d servers)\n", len(cfg.Pool.Servers))
	} else {
		fmt.Printf("Using database server at %s\n", cfg.Network.Address)
	}
	printBanner()
	repl(dbClient)
}

func printBanner() {
	fmt.Println("Available commands:")
	fmt.Println("  SET table key value")
	fmt.Println("  SET table key \"value with spaces\"")
	fmt.Println("  GET table key")
	fmt.Println("  DEL table key")
	fmt.Println("  TABLES")
	fmt.Println("  EXISTS table")
	fmt.Println("  KEYS table")
	fmt.Println("  TYPE table key")
	fmt.Println("  INCR table key [delta]")
	fmt.Println("  APPEND table key value")
	fmt.Println("  HSET table key field value")
	fmt.Println("  HGET table key field")
	fmt.Println("Values are typed: 42 int, 42.5 float, true bool, [1,2] array, {\"a\":1} map, anything else string")
	fmt.Println("Wrap a literal in single quotes when it contains quotes, spaces or backslashes: SET t conf '{\"a\":1}'")
	fmt.Println("Type 'exit' to quit")
	fmt.Println()
}

// repl reads command lines from stdin until end of input, "exit", or a transport failure.
func repl(dbClient *client.Client) {
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

		if input == "" || strings.HasPrefix(input, "#") {
			continue
		}

		response, sErr := dbClient.Raw(ctx, input)
		if sErr != nil {
			// A rejected command is an answer, not a broken session: print it and keep the prompt. Only a transport failure
			// ends the loop, because the connection can no longer be trusted to carry the next command.
			if serverErr, ok := errors.AsType[*client.ServerError](sErr); ok {
				fmt.Printf("%s %s\n", serverErr.Code, serverErr.Msg)
				continue
			}
			fmt.Printf("Failed to send command: %v\n", sErr)
			break
		}
		fmt.Println(response)
	}

	if err := scanner.Err(); err != nil {
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
