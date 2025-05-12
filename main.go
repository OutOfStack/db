package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))

	fmt.Println("Simple DB CLI")
	fmt.Println("Available commands:")
	fmt.Println("SET <key> <value> - Set a key-value pair")
	fmt.Println("GET <key> - Get value by key")
	fmt.Println("DEL <key> - Delete key")
	fmt.Println("Type 'exit' to quit")
	fmt.Println()

	compute := NewComputeLayer(NewParser(), NewStorageLayer(NewEngine()), logger)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		logger.Info("Received input", "input", input)

		// Exit command
		if input == "exit" {
			fmt.Println("Exiting...")
			return
		}

		output, err := compute.HandleRequest(ctx, input)
		if err != nil {
			logger.Error("Error handling request", "err", err)
			fmt.Printf("Error: %v\n", err)
		} else {
			logger.Info("Command executed successfully", "output", output)
			if output == "" {
				fmt.Println("OK")
			} else {
				fmt.Printf("Result: %s\n", output)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Error("Error reading input", "err", err)
		fmt.Printf("Error reading input: %v\n", err)
	}
}
