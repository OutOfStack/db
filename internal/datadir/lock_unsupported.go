//go:build !windows && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd

package datadir

import (
	"errors"
	"os"
)

func lockFile(string) (*os.File, error) {
	return nil, errors.New("data directory locking is unsupported on this operating system")
}
