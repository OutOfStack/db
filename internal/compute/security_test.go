package compute_test

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/OutOfStack/db/internal/compute"
	mocks "github.com/OutOfStack/db/internal/compute/mocks"
	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/parser"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCommandLogsRedactUserData(t *testing.T) {
	t.Parallel()
	const sentinel = "private-sentinel-9ea85"
	for _, level := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
		t.Run(level.String(), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: level}))
			c := compute.New(parser.New(), storage.New(engine.New()), logger)
			_, err := c.HandleRequest(t.Context(), "SET", []string{sentinel, sentinel, sentinel})
			require.NoError(t, err)
			_, err = c.HandleRequest(t.Context(), "GET", []string{sentinel, sentinel + "missing"})
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = c.HandleRequest(t.Context(), "SET", []string{sentinel, sentinel, "[" + sentinel})
			require.Error(t, err)
			_, err = c.HandleRequest(t.Context(), sentinel, nil)
			require.Error(t, err)
			out := buf.String()
			require.Contains(t, out, "cmd=SET")
			require.Contains(t, out, "outcome=ok")
			require.Contains(t, out, "outcome=not_found")
			require.Contains(t, out, "outcome=error")
			require.Contains(t, out, "outcome=parse_error")
			require.Contains(t, out, "duration=")
			require.Contains(t, out, "table_bytes=")
			require.Contains(t, out, "key_bytes=")
			if level == slog.LevelDebug {
				require.Contains(t, out, sentinel)
			} else {
				require.NotContains(t, out, sentinel)
				require.NotContains(t, out, "args=")
			}
		})
	}
}

func TestStorageErrorTextIsNotLoggedAtInfo(t *testing.T) {
	t.Parallel()
	const sentinel = "private-storage-error"
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStorage(ctrl)
	store.EXPECT().Execute(gomock.Any(), "GET", gomock.Any()).Return(protocol.Reply{}, errors.New(sentinel))
	var buf bytes.Buffer
	c := compute.New(parser.New(), store, slog.New(slog.NewTextHandler(&buf, nil)))
	_, err := c.HandleRequest(t.Context(), "GET", []string{"t", "k"})
	require.ErrorContains(t, err, sentinel)
	require.NotContains(t, buf.String(), sentinel)
	require.Contains(t, buf.String(), "outcome=error")
}
