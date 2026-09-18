package tiered

import (
	"log/slog"
	"os"
)

// OpenWithSync opens an engine whose fsync calls go through syncFile, so tests can inject fsync failures.
func OpenWithSync(cfg Config, logger *slog.Logger, syncFile func(*os.File) error) (*Engine, error) {
	return open(cfg, logger, syncFile)
}
