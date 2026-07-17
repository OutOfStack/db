//go:build !windows

package wal

import "os"

func syncDirectory(path string) error {
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
