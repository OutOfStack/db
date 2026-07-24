package compute_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/OutOfStack/db/internal/compute"
	mocks "github.com/OutOfStack/db/internal/compute/mocks"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestHandleRequest_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorage(ctrl)

	cmd := "echo"
	args := []string{"hello"}
	result := protocol.BulkString("hello")

	mockParser.EXPECT().Parse(cmd, args).Return(cmd, args, nil)
	mockStorage.EXPECT().Execute(gomock.Any(), cmd, args).Return(result, nil)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	c := compute.New(mockParser, mockStorage, logger)
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, cmd, args)
	require.NoError(t, err)
	require.Equal(t, result, res)
}

func TestHandleRequest_ParserError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorage(ctrl)

	cmd := "bad"
	args := []string{"input"}
	parseErr := errors.New("parse failed")

	mockParser.EXPECT().Parse(cmd, args).Return("", nil, parseErr)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	c := compute.New(mockParser, mockStorage, logger)
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, cmd, args)
	require.Error(t, err)
	require.Empty(t, res)
	require.Contains(t, err.Error(), "parse failed")
}

func TestHandleRequest_StorageError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorage(ctrl)

	cmd := "echo"
	args := []string{"hello"}
	storageErr := errors.New("storage failed")

	mockParser.EXPECT().Parse(cmd, args).Return(cmd, args, nil)
	mockStorage.EXPECT().Execute(gomock.Any(), cmd, args).Return(protocol.Reply{}, storageErr)

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	c := compute.New(mockParser, mockStorage, logger)
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, cmd, args)
	require.Error(t, err)
	require.Empty(t, res)
	require.Contains(t, err.Error(), "storage failed")
}

type fakeAdmin struct {
	promoted   bool
	statusCall bool
}

func (f *fakeAdmin) Promote(context.Context) (protocol.Reply, error) {
	f.promoted = true
	return protocol.SimpleString("OK"), nil
}

func (f *fakeAdmin) Status(context.Context) (protocol.Reply, error) {
	f.statusCall = true
	return protocol.BulkStringArray([]string{"role", "master"}), nil
}

func TestHandleRequest_AdminRouting(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Storage must never see admin commands.
	mockStorage := mocks.NewMockStorage(ctrl)
	admin := &fakeAdmin{}
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	c := compute.New(parser.New(), mockStorage, logger, compute.WithAdmin(admin))
	ctx := t.Context()

	res, err := c.HandleRequest(ctx, "PROMOTE", nil)
	require.NoError(t, err)
	require.Equal(t, "OK", res.Value)
	require.True(t, admin.promoted)

	res, err = c.HandleRequest(ctx, "REPLICATION", []string{"STATUS"})
	require.NoError(t, err)
	require.Equal(t, protocol.ReplyArray, res.Kind)
	require.True(t, admin.statusCall)
}

func TestHandleRequest_AdminDisabled(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mocks.NewMockStorage(ctrl)
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	c := compute.New(parser.New(), mockStorage, logger)

	_, err := c.HandleRequest(t.Context(), "PROMOTE", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "replication not enabled")
}
