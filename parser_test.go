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

// TestParser_Parse_Mock tests the Parse method of the Parser using the generated mock.
func TestParser_Parse_Mock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)

	tests := []struct {
		input    string
		cmd      string
		args     []string
		err      error
		wantCmd  string
		wantArgs []string
		wantErr  bool
	}{
		{"SET foo bar", "SET", []string{"foo", "bar"}, nil, "SET", []string{"foo", "bar"}, false},
		{"GET foo", "GET", []string{"foo"}, nil, "GET", []string{"foo"}, false},
		{"DEL foo", "DEL", []string{"foo"}, nil, "DEL", []string{"foo"}, false},
		{"SET foo", "", nil, errors.New("SET requires 2 arguments"), "", nil, true},
		{"GET", "", nil, errors.New("GET requires 1 argument"), "", nil, true},
		{"UNKNOWN foo", "", nil, errors.New("unknown command: UNKNOWN"), "", nil, true},
		{"", "", nil, errors.New("empty input"), "", nil, true},
	}

	for _, tt := range tests {
		mockParser.EXPECT().Parse(tt.input).Return(tt.cmd, tt.args, tt.err)
		cmd, args, err := mockParser.Parse(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("Parse(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
		}
		if cmd != tt.wantCmd {
			t.Errorf("Parse(%q) cmd = %q, want %q", tt.input, cmd, tt.wantCmd)
		}
		if len(args) != len(tt.wantArgs) {
			t.Errorf("Parse(%q) args = %v, want %v", tt.input, args, tt.wantArgs)
			continue
		}
		for i := range args {
			if args[i] != tt.wantArgs[i] {
				t.Errorf("Parse(%q) args[%d] = %q, want %q", tt.input, i, args[i], tt.wantArgs[i])
			}
		}
	}
}

// TestComputeLayer_WithMockParser demonstrates using the mock parser in ComputeLayer.
func TestComputeLayer_WithMockParser(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockParser := mocks.NewMockParser(ctrl)
	mockStorage := mocks.NewMockStorageLayer(ctrl)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	ctx := context.Background()

	mockParser.EXPECT().Parse("SET foo bar").Return("SET", []string{"foo", "bar"}, nil)
	mockStorage.EXPECT().Execute(ctx, "SET", []string{"foo", "bar"}).Return("OK", nil)

	compute := NewComputeLayer(mockParser, mockStorage, logger)
	out, err := compute.HandleRequest(ctx, "SET foo bar")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "OK" {
		t.Fatalf("unexpected output: got %q, want %q", out, "OK")
	}
}
