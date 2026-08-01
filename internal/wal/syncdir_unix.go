//go:build !windows

package wal

import "os"

// SyncDirectory fsyncs a directory so entries created in it (a new segment file) survive a crash, not just the file
// contents.
func SyncDirectory(path string) error {
	// #nosec G304 -- path is the operator-configured WAL directory
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
