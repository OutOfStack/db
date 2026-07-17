package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	walPrefix      = "wal-"
	walSuffix      = ".log"
	snapshotPrefix = "snapshot-"
	snapshotSuffix = ".db"
)

type numberedFile struct {
	path   string
	number uint64
}

func walFilename(firstLSN uint64) string {
	return fmt.Sprintf("%s%020d%s", walPrefix, firstLSN, walSuffix)
}

func snapshotFilename(lsn uint64) string {
	return fmt.Sprintf("%s%020d%s", snapshotPrefix, lsn, snapshotSuffix)
}

func listNumberedFiles(dir, prefix, suffix string) ([]numberedFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	files := make([]numberedFile, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}
		text := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		number, parseErr := strconv.ParseUint(text, 10, 64)
		if parseErr != nil {
			continue
		}
		files = append(files, numberedFile{path: filepath.Join(dir, name), number: number})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].number < files[j].number })
	return files, nil
}
