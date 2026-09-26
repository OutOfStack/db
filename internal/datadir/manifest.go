package datadir

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/wal"
)

// ManifestName is the file that makes a data directory self-describing: a copied directory says which engine and
// on-disk formats it holds, so a restore can be refused before recovery touches it rather than failing half-way.
const ManifestName = "MANIFEST"

// ManifestVersion is the version of the manifest file itself. A manifest carrying another version is refused.
const ManifestVersion = 1

// Format names used as keys of Manifest.Formats.
const (
	FormatWAL      = "wal"
	FormatSnapshot = "snapshot"
	FormatSegment  = "segment"
)

// Manifest describes a data directory. It is written as indented JSON; unknown fields are ignored on read, so a later
// v1.x release can add fields without making the directory unreadable to an earlier one.
type Manifest struct {
	// Version is ManifestVersion.
	Version int `json:"manifest_version"`
	// Engine is the engine whose files the directory holds: engine.TypeInMemory (WAL and snapshots) or
	// engine.TypeTiered (segments).
	Engine string `json:"engine"`
	// Formats maps each file kind the engine writes to its format version.
	Formats map[string]int `json:"formats"`
	// Sync is the fsync policy the directory was last written under. It is informational — a restart may change the
	// policy — and tells whoever restores a copy what durability the source promised.
	Sync string `json:"sync"`
	// Release is the release of the build that last wrote the manifest. It is informational.
	Release string `json:"release"`
}

// NewManifest describes a directory written by this build.
func NewManifest(engineType string, sync wal.SyncPolicy, release string) Manifest {
	return Manifest{
		Version: ManifestVersion,
		Engine:  engineType,
		Formats: Formats(engineType),
		Sync:    string(sync),
		Release: release,
	}
}

// Formats returns the format versions this build reads and writes for engineType's files.
func Formats(engineType string) map[string]int {
	if engineType == engine.TypeTiered {
		return map[string]int{FormatSegment: tiered.SegmentFormatVersion}
	}
	return map[string]int{FormatWAL: wal.WALFormatVersion, FormatSnapshot: wal.SnapshotFormatVersion}
}

// CheckManifest reports whether a directory described by m can be opened by this build as engineType. The engine and
// every format version have to match exactly: a directory written by another engine or in a format this build does not
// write is refused, with a message naming both sides, rather than handed to recovery.
func CheckManifest(dir string, m Manifest, engineType string) error {
	path := filepath.Join(dir, ManifestName)
	if m.Version != ManifestVersion {
		return fmt.Errorf("%s: manifest version %d is not supported (this build reads version %d)",
			path, m.Version, ManifestVersion)
	}
	if m.Engine != engineType {
		return fmt.Errorf("%s: data directory holds %s engine files, but the configuration selects %s",
			path, m.Engine, engineType)
	}
	want := Formats(engineType)
	if !maps.Equal(m.Formats, want) {
		return fmt.Errorf("%s: data directory formats %s do not match this build's %s; "+
			"use a build whose -version reports the directory's formats, or restore a backup written in this build's",
			path, formatList(m.Formats), formatList(want))
	}
	return nil
}

func formatList(formats map[string]int) string {
	parts := make([]string, 0, len(formats))
	for _, name := range slices.Sorted(maps.Keys(formats)) {
		parts = append(parts, fmt.Sprintf("%s=%d", name, formats[name]))
	}
	return strings.Join(parts, " ")
}

// ReadManifest reads dir's manifest. ok is false when the directory has none. A manifest that exists but cannot be
// parsed is an error: startup must not guess what a damaged description meant.
func ReadManifest(dir string) (Manifest, bool, error) {
	path := filepath.Join(dir, ManifestName)
	data, err := os.ReadFile(path) // #nosec G304 -- fixed name inside the operator-configured data directory
	if errors.Is(err, os.ErrNotExist) {
		return Manifest{}, false, nil
	}
	if err != nil {
		return Manifest{}, false, fmt.Errorf("read %s: %w", path, err)
	}
	var m Manifest
	if err = json.Unmarshal(data, &m); err != nil {
		return Manifest{}, false, fmt.Errorf("parse %s: %w", path, err)
	}
	return m, true, nil
}

// WriteManifest publishes m as dir's manifest atomically — a temporary file, fsynced, renamed over the old one, then the
// directory fsynced — so a crash leaves either the previous manifest or the new one, never a torn file. It does nothing
// when the file already holds exactly this content.
func WriteManifest(dir string, m Manifest) error {
	content, err := EncodeManifest(m)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, ManifestName)
	current, err := os.ReadFile(path) // #nosec G304 -- fixed name inside the operator-configured data directory
	if err == nil && bytes.Equal(current, content) {
		return nil
	}
	temporary, err := os.CreateTemp(dir, ManifestName+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	defer func() { _ = os.Remove(temporary.Name()) }()
	if _, err = temporary.Write(content); err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	if err = os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("publish manifest: %w", err)
	}
	if err = wal.SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync manifest: %w", err)
	}
	return nil
}

// EncodeManifest renders m exactly as WriteManifest stores it.
func EncodeManifest(m Manifest) ([]byte, error) {
	content, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode manifest: %w", err)
	}
	return append(content, '\n'), nil
}
