package datadir

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Kind identifies the durable database format present in a directory.
type Kind string

const (
	KindNone   Kind = "none"
	KindWAL    Kind = "wal"
	KindTiered Kind = "tiered"
	KindMixed  Kind = "mixed"
)

// Detect classifies files written by the WAL-backed and tiered engines. A valid database filename counts even when its
// header is empty or corrupt, so a damaged store cannot be mistaken for an unused directory.
func Detect(dir string) (Kind, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return KindNone, nil
		}
		return KindNone, fmt.Errorf("read data directory %q: %w", dir, err)
	}

	var walFiles, tieredFiles bool
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		walFiles = walFiles || numbered(name, "wal-", ".log", 64) || numbered(name, "snapshot-", ".db", 64)
		tieredFiles = tieredFiles || numbered(name, "seg-", ".data", 32)
	}
	switch {
	case walFiles && tieredFiles:
		return KindMixed, nil
	case walFiles:
		return KindWAL, nil
	case tieredFiles:
		return KindTiered, nil
	default:
		return KindNone, nil
	}
}

func numbered(name, prefix, suffix string, bits int) bool {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return false
	}
	number := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	_, err := strconv.ParseUint(number, 10, bits)
	return err == nil
}
