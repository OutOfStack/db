package tiered_test

import (
	"fmt"
	"testing"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

// TestListingsAreBounded checks the tiered engine applies the same reply limit as the in-memory one, and that its
// status reports the fsync the always policy performs.
func TestListingsAreBounded(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t.TempDir())
	cfg.Sync = wal.SyncAlways
	e := open(t, cfg)
	ctx := t.Context()
	for i := range 100 {
		require.NoError(t, e.Set(ctx, "users", fmt.Sprintf("key-%04d", i), "v"))
		require.NoError(t, e.Set(ctx, fmt.Sprintf("table-%04d", i), "k", "v"))
	}

	_, err := e.Keys(ctx, "users", 256)
	require.Equal(t, protocol.CodeTooLarge, protocol.CodeOf(err))
	_, err = e.Tables(ctx, 256)
	require.Equal(t, protocol.CodeTooLarge, protocol.CodeOf(err))

	keys, err := e.Keys(ctx, "users", 64<<10)
	require.NoError(t, err)
	require.Len(t, keys, 100)
	require.Equal(t, "key-0000", keys[0])

	require.False(t, e.Status().LastSync.IsZero(), "SyncAlways fsyncs every write")
}
