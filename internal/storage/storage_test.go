package storage_test

import (
	"errors"
	"testing"

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
		assert.Equal(t, "OK", result)
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
		assert.Equal(t, value, result)
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
		assert.Equal(t, "OK", result)
	})

	t.Run("invalid command", func(t *testing.T) {
		t.Parallel()

		s, _ := newStorageWithMock(t)

		result, err := s.Execute(t.Context(), "INVALID", []string{"table", "key"})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("not enough args for SET", func(t *testing.T) {
		t.Parallel()

		s, _ := newStorageWithMock(t)

		_, err := s.Execute(t.Context(), "SET", []string{"table", "key"})
		require.Error(t, err)
		assert.Equal(t, "not enough args", err.Error())
	})

	t.Run("not enough args for GET", func(t *testing.T) {
		t.Parallel()

		s, _ := newStorageWithMock(t)

		_, err := s.Execute(t.Context(), "GET", []string{"table"})
		require.Error(t, err)
		assert.Equal(t, "not enough args", err.Error())
	})

	t.Run("not enough args for DEL", func(t *testing.T) {
		t.Parallel()

		s, _ := newStorageWithMock(t)

		_, err := s.Execute(t.Context(), "DEL", []string{})
		require.Error(t, err)
		assert.Equal(t, "not enough args", err.Error())
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
