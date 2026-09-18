package datadir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/OutOfStack/db/internal/wal"
)

// unverifiedHistoryName marks a torn-tail recovery that may have lost records already streamed to standbys.
// It survives clean restarts until the operator clears it after reseeding every standby.
const unverifiedHistoryName = ".unverified-history"

// MarkUnverifiedHistory durably records reason in dir. An existing marker is kept: the first reason is the one the
// operator has yet to act on.
func MarkUnverifiedHistory(dir, reason string) error {
	path := filepath.Join(dir, unverifiedHistoryName)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check unverified-history marker: %w", err)
	}
	temporary, err := os.CreateTemp(dir, unverifiedHistoryName+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create unverified-history marker: %w", err)
	}
	defer func() { _ = os.Remove(temporary.Name()) }()
	if _, err = temporary.WriteString(reason); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write unverified-history marker: %w", err)
	}
	if err = os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("publish unverified-history marker: %w", err)
	}
	if err = wal.SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync unverified-history marker: %w", err)
	}
	return nil
}

// UnverifiedHistory returns the recorded reason and whether a marker exists.
func UnverifiedHistory(dir string) (string, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, unverifiedHistoryName)) // #nosec G304 -- fixed name inside the data directory
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read unverified-history marker: %w", err)
	}
	return string(data), true, nil
}

// ClearUnverifiedHistory removes the marker. It is the operator's statement that every standby has been reseeded.
func ClearUnverifiedHistory(dir string) error {
	if err := os.Remove(filepath.Join(dir, unverifiedHistoryName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove unverified-history marker: %w", err)
	}
	if err := wal.SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync data directory after clearing marker: %w", err)
	}
	return nil
}
