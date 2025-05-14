package compute_test

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/OutOfStack/db/internal/compute"
	mocks "github.com/OutOfStack/db/internal/compute/mocks"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestHandleRequest_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorage(ctrl)

	input := "echo hello"
	cmd := "echo"
	args := []string{"hello"}
	result := "hello"

	mockParser.EXPECT().Parse(input).Return(cmd, args, nil)
	mockStorage.EXPECT().Execute(gomock.Any(), cmd, args).Return(result, nil)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	c := compute.New(mockParser, mockStorage, logger)
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, input)
	require.NoError(t, err)
	require.Equal(t, result, res)
}

func TestHandleRequest_ParserError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorage(ctrl)

	input := "bad input"
	parseErr := errors.New("parse failed")

	mockParser.EXPECT().Parse(input).Return("", nil, parseErr)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	c := compute.New(mockParser, mockStorage, logger)
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, input)
	require.Error(t, err)
	require.Empty(t, res)
	require.Contains(t, err.Error(), "parse failed")
}

func TestHandleRequest_StorageError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorage(ctrl)

	input := "echo hello"
	cmd := "echo"
	args := []string{"hello"}
	storageErr := errors.New("storage failed")

	mockParser.EXPECT().Parse(input).Return(cmd, args, nil)
	mockStorage.EXPECT().Execute(gomock.Any(), cmd, args).Return("", storageErr)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	c := compute.New(mockParser, mockStorage, logger)
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, input)
	require.Error(t, err)
	require.Empty(t, res)
	require.Contains(t, err.Error(), "storage failed")
}
