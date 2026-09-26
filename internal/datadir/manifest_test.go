package datadir_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

func TestManifestRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	_, found, err := datadir.ReadManifest(dir)
	require.NoError(t, err)
	require.False(t, found)

	written := datadir.NewManifest(engine.TypeInMemory, wal.SyncAlways, "v1.2.3")
	require.NoError(t, datadir.WriteManifest(dir, written))
	read, found, err := datadir.ReadManifest(dir)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, written, read)
	require.Equal(t, map[string]int{
		datadir.FormatWAL:      wal.WALFormatVersion,
		datadir.FormatSnapshot: wal.SnapshotFormatVersion,
	}, read.Formats)
	require.NoError(t, datadir.CheckManifest(dir, read, engine.TypeInMemory))

	// Rewriting replaces it, and leaves no temporary file behind.
	require.NoError(t, datadir.WriteManifest(dir, datadir.NewManifest(engine.TypeInMemory, wal.SyncNo, "v1.2.4")))
	read, _, err = datadir.ReadManifest(dir)
	require.NoError(t, err)
	require.Equal(t, "no", read.Sync)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, datadir.ManifestName, entries[0].Name())

	// A manifest is not a database file: a directory holding only one still counts as unused.
	kind, err := datadir.Detect(dir)
	require.NoError(t, err)
	require.Equal(t, datadir.KindNone, kind)
}

func TestCheckManifestRefusesMismatches(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	inMemory := datadir.NewManifest(engine.TypeInMemory, wal.SyncEverySec, "v1.0.0")
	tieredManifest := datadir.NewManifest(engine.TypeTiered, wal.SyncEverySec, "v1.0.0")
	require.Equal(t, map[string]int{datadir.FormatSegment: tiered.SegmentFormatVersion}, tieredManifest.Formats)

	tests := []struct {
		name     string
		manifest datadir.Manifest
		engine   string
		contains string
	}{
		{name: "wrong engine", manifest: tieredManifest, engine: engine.TypeInMemory, contains: "holds tiered engine files"},
		{name: "wrong engine reversed", manifest: inMemory, engine: engine.TypeTiered, contains: "holds in_memory engine"},
		{
			name: "older WAL format",
			manifest: func() datadir.Manifest {
				m := datadir.NewManifest(engine.TypeInMemory, wal.SyncEverySec, "v0.9.0")
				m.Formats[datadir.FormatWAL] = wal.WALFormatVersion - 1
				return m
			}(),
			engine:   engine.TypeInMemory,
			contains: "do not match this build's",
		},
		{
			name: "missing format",
			manifest: func() datadir.Manifest {
				m := datadir.NewManifest(engine.TypeInMemory, wal.SyncEverySec, "v1.0.0")
				delete(m.Formats, datadir.FormatSnapshot)
				return m
			}(),
			engine:   engine.TypeInMemory,
			contains: "do not match",
		},
		{
			name: "future manifest version",
			manifest: func() datadir.Manifest {
				m := datadir.NewManifest(engine.TypeInMemory, wal.SyncEverySec, "v2.0.0")
				m.Version = datadir.ManifestVersion + 1
				return m
			}(),
			engine:   engine.TypeInMemory,
			contains: "manifest version 2 is not supported",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := datadir.CheckManifest(dir, tc.manifest, tc.engine)
			require.ErrorContains(t, err, tc.contains)
			require.ErrorContains(t, err, filepath.Join(dir, datadir.ManifestName), "the error names the file")
		})
	}
}

// TestReadManifestToleratesAdditiveFields lets a later v1.x add fields without making a directory unreadable to an
// earlier build, while a manifest that cannot be parsed at all is an error rather than an absent file.
func TestReadManifestToleratesAdditiveFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, datadir.ManifestName)

	require.NoError(t, os.WriteFile(path, []byte(`{"manifest_version":1,"engine":"in_memory",`+
		`"formats":{"wal":2,"snapshot":3},"sync":"always","release":"v1.9.0","added_later":true}`), 0o600))
	m, found, err := datadir.ReadManifest(dir)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "v1.9.0", m.Release)

	require.NoError(t, os.WriteFile(path, []byte(`{"manifest_version":`), 0o600))
	_, _, err = datadir.ReadManifest(dir)
	require.ErrorContains(t, err, "parse")
}
