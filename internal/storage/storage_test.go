package storage_test

import (
	"errors"
	"testing"

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
