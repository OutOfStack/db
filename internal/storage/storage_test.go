package storage_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	mocks "github.com/OutOfStack/db/internal/storage/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// newStorageWithMock creates a storage instance with its own mock engine so
// subtests stay independent and can run in parallel
func newStorageWithMock(t *testing.T) (*storage.Storage, *mocks.MockEngine) {
	t.Helper()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	return storage.New(mockEngine), mockEngine
}

type fakeWAL struct {
	append func(context.Context, string, []string) (uint64, error)
	last   uint64
	pruned uint64
}

func (f *fakeWAL) Append(ctx context.Context, command string, args []string) (uint64, error) {
	return f.append(ctx, command, args)
}

func (f *fakeWAL) LastLSN() uint64 { return f.last }

func (f *fakeWAL) Prune(_ context.Context, uptoLSN uint64) error {
	f.pruned = uptoLSN
	return nil
}

func TestStorage_WALBeforeMutation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	appended := false
	log := &fakeWAL{append: func(_ context.Context, command string, args []string) (uint64, error) {
		assert.Equal(t, "SET", command)
		assert.Equal(t, []string{"t", "k", "v"}, args)
		appended = true
		return 1, nil
	}}
	mockEngine.EXPECT().Set(ctx, "t", "k", "v").DoAndReturn(func(context.Context, string, string, string) error {
		assert.True(t, appended, "engine mutation happened before WAL append")
		return nil
	})

	result, err := storage.New(mockEngine, storage.WithWAL(log)).Execute(ctx, "SET", []string{"t", "k", "v"})
	require.NoError(t, err)
	assert.Equal(t, protocol.SimpleString("OK"), result)
}

func TestStorage_WALFailurePreventsMutation(t *testing.T) {
	t.Parallel()
	expectedErr := errors.New("disk full")
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	log := &fakeWAL{append: func(context.Context, string, []string) (uint64, error) { return 0, expectedErr }}

	result, err := storage.New(mockEngine, storage.WithWAL(log)).Execute(t.Context(), "DEL", []string{"t", "k"})
	require.ErrorIs(t, err, expectedErr)
	assert.Empty(t, result)
}

// TestStorage_ConcurrentMutationsApplyInLSNOrder verifies that when many
// mutations append concurrently (so the WAL can group-commit them), they still
// land in the engine in LSN order: the surviving value is the highest-LSN write.
func TestStorage_ConcurrentMutationsApplyInLSNOrder(t *testing.T) {
	t.Parallel()
	eng := engine.New()

	var mu sync.Mutex
	var lsn uint64
	valueByLSN := make(map[uint64]string)
	log := &fakeWAL{append: func(_ context.Context, _ string, args []string) (uint64, error) {
		mu.Lock()
		lsn++
		assigned := lsn
		valueByLSN[assigned] = args[2]
		mu.Unlock()
		// Jitter so appends return out of order relative to their LSNs; the apply
		// gate must still serialize the engine writes by LSN.
		time.Sleep(time.Duration(assigned%3) * time.Millisecond)
		return assigned, nil
	}}
	store := storage.New(eng, storage.WithWAL(log))

	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Execute(context.Background(), "SET", []string{"t", "k", fmt.Sprintf("v%d", i)})
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	mu.Lock()
	want := valueByLSN[lsn]
	mu.Unlock()
	got, err := eng.Get(context.Background(), "t", "k")
	require.NoError(t, err)
	assert.Equal(t, want, got, "engine must reflect the highest-LSN write")
}

func TestStorage_SnapshotUsesMatchingLSNAndPrunes(t *testing.T) {
	t.Parallel()
	mockEngine := mocks.NewMockEngine(gomock.NewController(t))
	log := &fakeWAL{last: 17, append: func(context.Context, string, []string) (uint64, error) { return 0, nil }}
	mockEngine.EXPECT().Range(gomock.Any()).Do(func(fn func(string, string, string) bool) {
		fn("t", "k", "v")
	})
	store := storage.New(mockEngine, storage.WithWAL(log))

	err := store.Snapshot(t.Context(), func(_ context.Context, lsn uint64, source storage.SnapshotSource) error {
		assert.Equal(t, uint64(17), lsn)
		source.Range(func(table, key, value string) bool {
			assert.Equal(t, "t", table)
			assert.Equal(t, "k", key)
			assert.Equal(t, "v", value)
			return true
		})
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(17), log.pruned)
}

func TestStorage_Execute(t *testing.T) {
	t.Parallel()

	t.Run("SET command", func(t *testing.T) {
		t.Parallel()

		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		table := "test_table"
		key := "test_key"
		value := "test_value"

		// Mock the Set call
		mockEngine.EXPECT().Set(ctx, table, key, value).Return(nil)

		result, err := s.Execute(ctx, "SET", []string{table, key, value})
		require.NoError(t, err)
		assert.Equal(t, protocol.SimpleString("OK"), result)
	})

	t.Run("GET command", func(t *testing.T) {
		t.Parallel()

		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		table := "test_table"
		key := "test_key"
		value := "test_value"

		// Mock the Get call
		mockEngine.EXPECT().Get(ctx, table, key).Return(value, nil)

		result, err := s.Execute(ctx, "GET", []string{table, key})
		require.NoError(t, err)
		assert.Equal(t, protocol.BulkString(value), result)
	})

	t.Run("DEL command", func(t *testing.T) {
		t.Parallel()

		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		table := "test_table"
		key := "test_key"

		// Mock the Del call
		mockEngine.EXPECT().Del(ctx, table, key).Return(nil)

		result, err := s.Execute(ctx, "DEL", []string{table, key})
		require.NoError(t, err)
		assert.Equal(t, protocol.SimpleString("OK"), result)
	})

	t.Run("TABLES command", func(t *testing.T) {
		t.Parallel()
		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		mockEngine.EXPECT().Tables(ctx).Return([]string{"orders", "users"})
		result, err := s.Execute(ctx, "TABLES", nil)
		require.NoError(t, err)
		assert.Equal(t, protocol.BulkStringArray([]string{"orders", "users"}), result)
	})

	t.Run("EXISTS command", func(t *testing.T) {
		t.Parallel()
		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		mockEngine.EXPECT().TableExists(ctx, "users").Return(true)
		result, err := s.Execute(ctx, "EXISTS", []string{"users"})
		require.NoError(t, err)
		assert.Equal(t, protocol.BulkString("true"), result)
	})

	t.Run("KEYS command", func(t *testing.T) {
		t.Parallel()
		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		mockEngine.EXPECT().Keys(ctx, "users").Return([]string{"a", "z"})
		result, err := s.Execute(ctx, "KEYS", []string{"users"})
		require.NoError(t, err)
		assert.Equal(t, protocol.BulkStringArray([]string{"a", "z"}), result)
	})

	t.Run("invalid command", func(t *testing.T) {
		t.Parallel()

		s, _ := newStorageWithMock(t)

		result, err := s.Execute(t.Context(), "INVALID", []string{"table", "key"})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("engine error on SET", func(t *testing.T) {
		t.Parallel()

		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		table := "test_table"
		key := "test_key"
		value := "test_value"
		expectedErr := errors.New("engine error")

		// Mock the Set call to return an error
		mockEngine.EXPECT().Set(ctx, table, key, value).Return(expectedErr)

		result, err := s.Execute(ctx, "SET", []string{table, key, value})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})

	t.Run("engine error on GET", func(t *testing.T) {
		t.Parallel()

		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		table := "test_table"
		key := "test_key"
		expectedErr := errors.New("engine error")

		// Mock the Get call to return an error
		mockEngine.EXPECT().Get(ctx, table, key).Return("", expectedErr)

		result, err := s.Execute(ctx, "GET", []string{table, key})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})

	t.Run("engine error on DEL", func(t *testing.T) {
		t.Parallel()

		s, mockEngine := newStorageWithMock(t)
		ctx := t.Context()
		table := "test_table"
		key := "test_key"
		expectedErr := errors.New("engine error")

		// Mock the Del call to return an error
		mockEngine.EXPECT().Del(ctx, table, key).Return(expectedErr)

		result, err := s.Execute(ctx, "DEL", []string{table, key})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})
}
