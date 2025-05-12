package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/OutOfStack/db/internal/compute"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/storage"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	fmt.Println("Simple DB CLI")
	fmt.Println("Available commands:")
	fmt.Println("  SET key value")
	fmt.Println("  GET key")
	fmt.Println("  DEL key")
	fmt.Println("Type 'exit' to quit")
	fmt.Println()

	parser := parser.New()
	engine := engine.New()
	storage := storage.New(engine)
	compute := compute.New(parser, storage, logger)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "exit" {
			return
		}

		output, err := compute.HandleRequest(context.Background(), input)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			logger.Info("Command executed successfully", "output", output)

			if output == "" {
				fmt.Println("OK")
			} else {
				fmt.Println(output)
			}
		}
	}
}
