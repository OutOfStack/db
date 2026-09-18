package datadir_test

import (
	"testing"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/stretchr/testify/require"
)

func TestUnverifiedHistoryMarkerRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	reason, ok, err := datadir.UnverifiedHistory(dir)
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, reason)

	require.NoError(t, datadir.MarkUnverifiedHistory(dir, "first"))
	require.NoError(t, datadir.MarkUnverifiedHistory(dir, "second"), "a second mark keeps the first reason")
	reason, ok, err = datadir.UnverifiedHistory(dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "first", reason)
	// The marker is not a database file, so it never makes the directory look populated to the engine checks.
	kind, err := datadir.Detect(dir)
	require.NoError(t, err)
	require.Equal(t, datadir.KindNone, kind)

	require.NoError(t, datadir.ClearUnverifiedHistory(dir))
	require.NoError(t, datadir.ClearUnverifiedHistory(dir), "clearing twice is harmless")
	_, ok, err = datadir.UnverifiedHistory(dir)
	require.NoError(t, err)
	require.False(t, ok)
}
