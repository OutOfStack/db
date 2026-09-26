package version_test

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/version"
	"github.com/OutOfStack/db/internal/wal"
)

// TestInfoReportsEveryIdentity checks that Get reports each identity from the package that owns it, so a format version
// bumped at its source cannot leave -version claiming the old one.
func TestInfoReportsEveryIdentity(t *testing.T) {
	t.Parallel()

	info := version.Get()
	if info.Release == "" || info.Commit == "" {
		t.Errorf("release/commit = %q/%q, want non-empty (an unstamped build reports dev/unknown)", info.Release, info.Commit)
	}
	if info.Protocol != protocol.Version {
		t.Errorf("protocol = %q, want %q", info.Protocol, protocol.Version)
	}
	if info.WALFormat != wal.WALFormatVersion {
		t.Errorf("wal format = %d, want %d", info.WALFormat, wal.WALFormatVersion)
	}
	if info.SnapshotFormat != wal.SnapshotFormatVersion {
		t.Errorf("snapshot format = %d, want %d", info.SnapshotFormat, wal.SnapshotFormatVersion)
	}
	if info.SegmentFormat != tiered.SegmentFormatVersion {
		t.Errorf("segment format = %d, want %d", info.SegmentFormat, tiered.SegmentFormatVersion)
	}
	if info.Go != runtime.Version() {
		t.Errorf("go = %q, want %q", info.Go, runtime.Version())
	}

	rendered := info.String()
	for _, want := range []string{"release:", "commit:", "protocol:", "storage:", "go:"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("rendered identity is missing %q:\n%s", want, rendered)
		}
	}
}

// TestBinariesPrintVersion builds both binaries the way the Makefile does and checks that -version prints every
// identity, including the release and commit injected at link time. It builds, rather than calling the flag package,
// because the wiring it verifies — the -ldflags variable paths — only exists in a real build.
func TestBinariesPrintVersion(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("builds both binaries")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skipf("no go toolchain on PATH: %v", err)
	}

	const (
		wantRelease = "v9.9.9-test"
		wantCommit  = "0123456789abcdef"
	)
	ldflags := "-X github.com/OutOfStack/db/internal/version.release=" + wantRelease +
		" -X github.com/OutOfStack/db/internal/version.commit=" + wantCommit

	for _, pkg := range []string{"db", "db-cli"} {
		t.Run(pkg, func(t *testing.T) {
			t.Parallel()

			binary := filepath.Join(t.TempDir(), pkg)
			build := exec.CommandContext(t.Context(), "go", "build", "-ldflags", ldflags, "-o", binary,
				"github.com/OutOfStack/db/cmd/"+pkg)
			if out, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build %s: %v\n%s", pkg, err, out)
			}

			out, err := exec.CommandContext(t.Context(), binary, "-version").CombinedOutput()
			if err != nil {
				t.Fatalf("%s -version: %v\n%s", pkg, err, out)
			}
			got := string(out)
			for _, want := range []string{
				wantRelease,
				wantCommit,
				protocol.Version,
				"wal=" + strconv.Itoa(wal.WALFormatVersion),
				"snapshot=" + strconv.Itoa(wal.SnapshotFormatVersion),
				"segment=" + strconv.Itoa(tiered.SegmentFormatVersion),
			} {
				if !strings.Contains(got, want) {
					t.Errorf("%s -version output is missing %q:\n%s", pkg, want, got)
				}
			}
		})
	}
}
