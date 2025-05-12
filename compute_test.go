package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"go.uber.org/mock/gomock"
	"github.com/OutOfStack/db/mocks"
)

// TestComputeLayer_HandleRequest tests the HandleRequest method of the ComputeLayer implementation using mocks.
func TestComputeLayer_HandleRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	tests := []struct {
		name      string
		setup     func(*mocks.MockParser, *mocks.MockStorageLayer)
		input     string
		wantOut   string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(mp *mocks.MockParser, ms *mocks.MockStorageLayer) {
				mp.EXPECT().Parse("SET foo bar").Return("SET", []string{"foo", "bar"}, nil)
				ms.EXPECT().Execute(ctx, "SET", []string{"foo", "bar"}).Return("OK", nil)
			},
			input:   "SET foo bar",
			wantOut: "OK",
			wantErr: false,
		},
		{
			name: "parse error",
			setup: func(mp *mocks.MockParser, ms *mocks.MockStorageLayer) {
				mp.EXPECT().Parse("bad").Return("", nil, errors.New("parse error"))
			},
			input:   "bad",
			wantOut: "",
			wantErr: true,
		},
		{
			name: "storage error",
			setup: func(mp *mocks.MockParser, ms *mocks.MockStorageLayer) {
				mp.EXPECT().Parse("GET foo").Return("GET", []string{"foo"}, nil)
				ms.EXPECT().Execute(ctx, "GET", []string{"foo"}).Return("", errors.New("not found"))
			},
			input:   "GET foo",
			wantOut: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		mp := mocks.NewMockParser(ctrl)
		ms := mocks.NewMockStorageLayer(ctrl)
		if tt.setup != nil {
			tt.setup(mp, ms)
		}
		c := NewComputeLayer(mp, ms, logger)
		out, err := c.HandleRequest(ctx, tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("%s: HandleRequest() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
		if out != tt.wantOut {
			t.Errorf("%s: HandleRequest() = %q, want %q", tt.name, out, tt.wantOut)
		}
	}
}
