//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package datadir

import (
	"os"
	"syscall"
)

func lockFile(path string) (*os.File, error) {
	// #nosec G304 -- path is the fixed lock filename inside the operator-configured data directory.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
