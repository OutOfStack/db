package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/network"
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

	client, err := network.NewTCPClient(cfg.Network.Address,
		network.WithClientIdleTimeout(cfg.Network.IdleTimeout),
		network.WithClientBufferSize(cfg.Network.MaxMessageSizeKB*1024),
	)
	if err != nil {
		fmt.Printf("Failed to connect to database server at %s: %v\n", cfg.Network.Address, err)
		os.Exit(1)
	}
	defer func() {
		if err = client.Close(); err != nil {
			fmt.Printf("Failed to close connection: %v\n", err)
		}
	}()

	fmt.Printf("Connected to database server at %s\n", cfg.Network.Address)
	fmt.Println("Available commands:")
	fmt.Println("  SET key value")
	fmt.Println("  GET key")
	fmt.Println("  DEL key")
	fmt.Println("Type 'exit' to quit")
	fmt.Println()

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

		response, sErr := client.Send([]byte(input + "\n"))
		if sErr != nil {
			fmt.Printf("Failed to send command: %v\n", err)
			break
		}
		fmt.Printf("%s\n", strings.TrimSpace(string(response)))
	}

	if err = scanner.Err(); err != nil {
		fmt.Printf("Input error: %v\n", err)
	}
}
