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

func TestStorage_Execute(t *testing.T) {
	ctx := t.Context()
	mockCtrl := gomock.NewController(t)
	mockEngine := mocks.NewMockEngine(mockCtrl)
	storage := storage.New(mockEngine)

	t.Run("SET command", func(t *testing.T) {
		key := "test_key"
		value := "test_value"

		// Mock the Set call
		mockEngine.EXPECT().Set(ctx, key, value).Return(nil)

		result, err := storage.Execute(ctx, "SET", []string{key, value})
		require.NoError(t, err)
		assert.Equal(t, "OK", result)
	})

	t.Run("GET command", func(t *testing.T) {
		key := "test_key"
		value := "test_value"

		// Mock the Get call
		mockEngine.EXPECT().Get(ctx, key).Return(value, nil)

		result, err := storage.Execute(ctx, "GET", []string{key})
		require.NoError(t, err)
		assert.Equal(t, value, result)
	})

	t.Run("DEL command", func(t *testing.T) {
		key := "test_key"

		// Mock the Del call
		mockEngine.EXPECT().Del(ctx, key).Return(nil)

		result, err := storage.Execute(ctx, "DEL", []string{key})
		require.NoError(t, err)
		assert.Equal(t, "OK", result)
	})

	t.Run("invalid command", func(t *testing.T) {
		result, err := storage.Execute(ctx, "INVALID", []string{"key"})
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("not enough args for SET", func(t *testing.T) {
		_, err := storage.Execute(ctx, "SET", []string{"key"})
		require.Error(t, err)
		assert.Equal(t, "not enough args", err.Error())
	})

	t.Run("not enough args for GET", func(t *testing.T) {
		_, err := storage.Execute(ctx, "GET", []string{})
		require.Error(t, err)
		assert.Equal(t, "not enough args", err.Error())
	})

	t.Run("not enough args for DEL", func(t *testing.T) {
		_, err := storage.Execute(ctx, "DEL", []string{})
		require.Error(t, err)
		assert.Equal(t, "not enough args", err.Error())
	})

	t.Run("engine error on SET", func(t *testing.T) {
		key := "test_key"
		value := "test_value"
		expectedErr := errors.New("engine error")

		// Mock the Set call to return an error
		mockEngine.EXPECT().Set(ctx, key, value).Return(expectedErr)

		result, err := storage.Execute(ctx, "SET", []string{key, value})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})

	t.Run("engine error on GET", func(t *testing.T) {
		key := "test_key"
		expectedErr := errors.New("engine error")

		// Mock the Get call to return an error
		mockEngine.EXPECT().Get(ctx, key).Return("", expectedErr)

		result, err := storage.Execute(ctx, "GET", []string{key})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})

	t.Run("engine error on DEL", func(t *testing.T) {
		key := "test_key"
		expectedErr := errors.New("engine error")

		// Mock the Del call to return an error
		mockEngine.EXPECT().Del(ctx, key).Return(expectedErr)

		result, err := storage.Execute(ctx, "DEL", []string{key})
		require.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Empty(t, result)
	})
}
