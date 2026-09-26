// Package version reports what a running binary is: its release, the commit it was built from, and the wire and
// on-disk formats it reads. Both binaries print it for -version, and an operator comparing two nodes — or a bug
// report — needs all of it, so the format versions are taken from the packages that define them rather than restated
// here.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

// release and commit are set at build time with -ldflags -X (see the Makefile). An unstamped build reports "dev" and
// falls back to the commit the Go toolchain recorded, so a `go build` or `go install` binary still identifies itself.
var (
	release = "dev" //nolint:gochecknoglobals // build-time identity, injected with -ldflags -X
	commit  = ""    //nolint:gochecknoglobals // build-time identity, injected with -ldflags -X
)

const unknownCommit = "unknown"

// Info is the full identity of a build.
type Info struct {
	// Release is the released version, or "dev" for an unstamped build.
	Release string
	// Commit is the source revision, suffixed "+dirty" when the tree had uncommitted changes.
	Commit string
	// Protocol is the wire protocol the server speaks.
	Protocol string
	// WALFormat, SnapshotFormat and SegmentFormat are the on-disk format versions this build reads and writes. A file
	// carrying any other version is refused at open rather than parsed.
	WALFormat      int
	SnapshotFormat int
	SegmentFormat  int
	// Go is the toolchain that built the binary.
	Go string
}

// Get returns this build's identity.
func Get() Info {
	return Info{
		Release:        release,
		Commit:         resolveCommit(),
		Protocol:       protocol.Version,
		WALFormat:      wal.WALFormatVersion,
		SnapshotFormat: wal.SnapshotFormatVersion,
		SegmentFormat:  tiered.SegmentFormatVersion,
		Go:             runtime.Version(),
	}
}

// String renders the identity of binary as the multi-line block -version prints.
func (i Info) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "release:  %s\n", i.Release)
	fmt.Fprintf(&b, "commit:   %s\n", i.Commit)
	fmt.Fprintf(&b, "protocol: %s\n", i.Protocol)
	fmt.Fprintf(&b, "storage:  wal=%d snapshot=%d segment=%d\n", i.WALFormat, i.SnapshotFormat, i.SegmentFormat)
	fmt.Fprintf(&b, "go:       %s\n", i.Go)
	return b.String()
}

// resolveCommit prefers the injected commit and otherwise reads the revision the toolchain stamped into the build info.
func resolveCommit() string {
	if commit != "" {
		return commit
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return unknownCommit
	}
	var revision, modified string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}
	if revision == "" {
		return unknownCommit
	}
	if modified == "true" {
		return revision + "+dirty"
	}
	return revision
}
