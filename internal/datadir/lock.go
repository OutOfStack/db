// Package datadir coordinates exclusive process ownership and identifies database files in a data directory.
package datadir

import (
	"fmt"
	"os"
	"path/filepath"
)

const lockName = ".db.lock"

// Lock holds exclusive process ownership of a data directory until Close.
type Lock struct{ file *os.File }

// Acquire takes the directory lock without waiting.
func Acquire(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory %q: %w", dir, err)
	}
	file, err := lockFile(filepath.Join(dir, lockName))
	if err != nil {
		return nil, fmt.Errorf("lock data directory %q: %w", dir, err)
	}
	return &Lock{file: file}, nil
}

// Close releases ownership. The lockfile remains so another process cannot replace the locked inode by racing cleanup.
func (l *Lock) Close() error {
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("release data directory lock: %w", err)
	}
	return nil
}
