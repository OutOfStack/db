package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/OutOfStack/db/client"
	"github.com/OutOfStack/db/internal/config"
	"github.com/OutOfStack/db/internal/version"
)

// Exit codes. A script can rely on exitOK meaning every command it piped in succeeded.
const (
	exitOK = 0
	// exitFailure reports that at least one command failed: a line the CLI could not parse, a transport failure, or an
	// error reply from the server. The run also ends with it when reading stdin fails.
	exitFailure = 1
	// exitUsage reports invalid flags or configuration; no command was sent.
	exitUsage = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, isTerminal(os.Stdin)))
}

// run is the whole CLI behind main, parameterized over its streams so tests can drive it. interactive selects the
// banner and prompt; piped input gets neither, so its output is only the replies.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer, interactive bool) int {
	flags := flag.NewFlagSet("db-cli", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var configPath, address string
	var showVersion, quiet bool
	var timeout time.Duration
	flags.StringVar(&configPath, "config", "", "Path to configuration file")
	flags.BoolVar(&showVersion, "version", false, "Print release, commit, protocol and storage format versions, then exit")
	flags.StringVar(&address, "address", "", "Database server address (overrides config)")
	flags.DurationVar(&timeout, "timeout", 0, "Connection idle timeout (overrides config)")
	flags.BoolVar(&quiet, "q", false, "Quiet: print only errors, for scripts that rely on the exit status")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if flags.NArg() > 0 {
		printf(stderr, "Unexpected arguments: %s (commands are read from stdin)\n", strings.Join(flags.Args(), " "))
		return exitUsage
	}

	if showVersion {
		printf(stdout, "%s", version.Get())
		return exitOK
	}

	cfg, err := config.LoadClientConfig(configPath)
	if err != nil {
		printf(stderr, "Failed to load configuration: %v\n", err)
		return exitUsage
	}
	if address != "" {
		cfg.Network.Address = address
	}
	if timeout > 0 {
		cfg.Network.IdleTimeout = timeout
	}

	// the client connects on first use, so an unreachable server surfaces on the first command rather than here
	dbClient, err := client.New(clientOptions(cfg)...)
	if err != nil {
		printf(stderr, "Invalid client configuration: %v\n", err)
		return exitUsage
	}

	s := &session{
		client:      dbClient,
		stdout:      stdout,
		stderr:      stderr,
		interactive: interactive && !quiet,
		quiet:       quiet,
	}
	if s.interactive {
		if cfg.Pool.Enabled {
			printf(stdout, "Using database pool (%d servers)\n", len(cfg.Pool.Servers))
		} else {
			printf(stdout, "Using database server at %s\n", cfg.Network.Address)
		}
		printBanner(stdout)
	}
	code := s.run(stdin)
	if err = dbClient.Close(); err != nil {
		printf(stderr, "Failed to close connection: %v\n", err)
		code = exitFailure
	}
	return code
}

func printBanner(w io.Writer) {
	for _, line := range []string{
		"Available commands:",
		"  SET table key value",
		"  SET table key \"value with spaces\"",
		"  GET table key",
		"  DEL table key",
		"  TABLES",
		"  EXISTS table",
		"  KEYS table",
		"  TYPE table key",
		"  INCR table key [delta]",
		"  APPEND table key value",
		"  HSET table key field value",
		"  HGET table key field",
		"  PING",
		"  STATUS",
		"Values are typed: 42 int, 42.5 float, true bool, [1,2] array, {\"a\":1} map, anything else string",
		"Wrap a literal in single quotes when it contains quotes, spaces or backslashes: SET t conf '{\"a\":1}'",
		"Type 'exit' to quit",
		"",
	} {
		printf(w, "%s\n", line)
	}
}

// session reads command lines and runs them one at a time.
type session struct {
	client      *client.Client
	stdout      io.Writer
	stderr      io.Writer
	interactive bool // print a prompt before each line
	quiet       bool // print errors only
}

// run reads command lines until end of input, "exit", or a failure that ends the session, and returns the exit code.
//
// An error reply is an answer, not a broken session: it is reported and the next line runs, so a script sees every
// failure it contains. Anything else — a line the CLI cannot parse, a transport failure — ends the session, because the
// commands after it were written assuming it ran. Either way the exit code is nonzero once any command has failed.
func (s *session) run(stdin io.Reader) int {
	ctx := context.Background()
	scanner := bufio.NewScanner(stdin)
	code := exitOK
	for line := 1; ; line++ {
		if s.interactive {
			printf(s.stdout, "> ")
		}
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

		response, err := s.client.Raw(ctx, input)
		if err == nil {
			if !s.quiet {
				printf(s.stdout, "%s\n", response)
			}
			continue
		}
		code = exitFailure
		if serverErr, ok := errors.AsType[*client.ServerError](err); ok {
			s.reportf(line, "%s %s", serverErr.Code, serverErr.Msg)
			continue
		}
		s.reportf(line, "Failed to send command: %v", err)
		return code
	}

	if err := scanner.Err(); err != nil {
		printf(s.stderr, "Input error: %v\n", err)
		return exitFailure
	}
	return code
}

// reportf prints an error. Piped input has no prompt to tie the error to its command, so the line number stands in.
func (s *session) reportf(line int, format string, args ...any) {
	if !s.interactive {
		format = fmt.Sprintf("line %d: %s", line, format)
	}
	printf(s.stderr, format+"\n", args...)
}

// printf writes to one of the CLI's output streams. A write error is dropped: with stdout or stderr failing there is
// nowhere left to report it, and the exit code already accounts for every command that failed.
func printf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func isTerminal(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
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
