package storage_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	mocks "github.com/OutOfStack/db/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// TestStorage_ListingsUseTheConfiguredLimit checks that the storage hands its reply limit to the engine and passes a
// refusal through with its code intact, for both listing commands.
func TestStorage_ListingsUseTheConfiguredLimit(t *testing.T) {
	t.Parallel()

	tooLarge := protocol.NewError(protocol.CodeTooLarge, "listing too large")
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	store := storage.New(mockEngine, storage.WithListLimit(4096))
	ctx := t.Context()

	mockEngine.EXPECT().Keys(ctx, "users", 4096).Return(nil, tooLarge)
	_, err := store.Execute(ctx, "KEYS", []string{"users"})
	assert.Equal(t, protocol.CodeTooLarge, protocol.CodeOf(err))

	mockEngine.EXPECT().Tables(ctx, 4096).Return(nil, tooLarge)
	_, err = store.Execute(ctx, "TABLES", nil)
	assert.Equal(t, protocol.CodeTooLarge, protocol.CodeOf(err))
}

// TestStorage_ListingLimitEndToEnd runs a real engine through the storage layer: a listing past the limit is refused,
// one under it is served in full.
func TestStorage_ListingLimitEndToEnd(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	store := storage.New(engine.New(), storage.WithListLimit(256))
	for i := range 50 {
		_, err := store.Execute(ctx, "SET", []string{"big", fmt.Sprintf("key-%04d", i), "v"})
		require.NoError(t, err)
	}
	_, err := store.Execute(ctx, "SET", []string{"small", "k", "v"})
	require.NoError(t, err)

	_, err = store.Execute(ctx, "KEYS", []string{"big"})
	require.Equal(t, protocol.CodeTooLarge, protocol.CodeOf(err))
	reply, err := store.Execute(ctx, "KEYS", []string{"small"})
	require.NoError(t, err)
	assert.Equal(t, protocol.BulkStringArray([]string{"k"}), reply)
	reply, err = store.Execute(ctx, "TABLES", nil)
	require.NoError(t, err)
	assert.Equal(t, protocol.BulkStringArray([]string{"big", "small"}), reply)
}

func TestStorage_StatusTracksSnapshots(t *testing.T) {
	t.Parallel()

	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	mockEngine.EXPECT().Range(gomock.Any()).AnyTimes()
	log := &fakeWAL{last: 17, append: func(context.Context, string, []string) (uint64, error) { return 0, nil }}
	store := storage.New(mockEngine, storage.WithWAL(log), storage.WithSnapshotLSN(9))

	// Before this process writes one, the status reports the snapshot recovery loaded, with no time.
	status := store.Status()
	assert.Equal(t, uint64(9), status.SnapshotLSN)
	assert.True(t, status.LastSnapshot.IsZero())

	// A failed write leaves the previous snapshot in place and reports degraded maintenance.
	writeErr := errors.New("disk full")
	require.ErrorIs(t, store.Snapshot(t.Context(), func(context.Context, uint64, storage.SnapshotSource) error {
		return writeErr
	}), writeErr)
	status = store.Status()
	assert.Equal(t, uint64(9), status.SnapshotLSN)
	assert.True(t, status.Degraded)

	require.NoError(t, store.Snapshot(t.Context(), func(context.Context, uint64, storage.SnapshotSource) error {
		return nil
	}))
	status = store.Status()
	assert.Equal(t, uint64(17), status.SnapshotLSN)
	assert.False(t, status.LastSnapshot.IsZero())
	assert.False(t, status.Degraded, "a successful snapshot clears degraded maintenance")
}
