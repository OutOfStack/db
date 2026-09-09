package tiered_test

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/OutOfStack/db/internal/engine/tiered"
	"github.com/stretchr/testify/require"
)

func TestRangeReadFailureRedactsIdentifiers(t *testing.T) {
	t.Parallel()
	const table, key = "private-table", "private-key"
	var logs bytes.Buffer
	cfg := testConfig(t.TempDir())
	cfg.MaxMemoryBytes = 128
	e, err := tiered.Open(cfg, slog.New(slog.NewTextHandler(&logs, nil)))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	require.NoError(t, e.Set(t.Context(), table, key, strings.Repeat("private-value", 128)))
	// The value exceeds the cache budget, so truncating its segment forces Range through the read-error path.
	require.NoError(t, os.Truncate(lastSegment(t, cfg.Dir), 0))
	called := false
	e.Range(func(_, _, _ string) bool {
		called = true
		return true
	})
	require.False(t, called)
	out := logs.String()
	require.Contains(t, out, "level=ERROR msg=\"Range read failed\"")
	require.Contains(t, out, "table_bytes=13")
	require.Contains(t, out, "key_bytes=11")
	require.Contains(t, out, "read value from segment")
	require.NotContains(t, out, table)
	require.NotContains(t, out, key)
	require.NotContains(t, out, "private-value")
}
