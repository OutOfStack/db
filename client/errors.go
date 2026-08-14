package client

import (
	"errors"

	"github.com/OutOfStack/db/internal/network"
)

// ErrNotFound is returned by Get and Del when the key does not exist
var ErrNotFound = errors.New("not found")

// ErrOutcomeUnknown reports that a command reached a server in full but no reply came back, so whether it took effect
// cannot be determined from here. The client never repeats such a command on its own, because commands like Incr and
// Append would then apply twice.
//
// Retrying is the caller's decision and depends on the command: Get, Del and an idempotent Set can simply be re-issued,
// while Incr, Append and HSet have to be checked against the server first. When cancellation caused it, the error also
// matches context.Canceled or context.DeadlineExceeded — a cancelled mutation may still have been applied.
var ErrOutcomeUnknown = network.ErrOutcomeUnknown

// ServerError represents an error message returned by the server
type ServerError struct {
	Msg string
}

// Error implements the error interface
func (e *ServerError) Error() string {
	return "server error: " + e.Msg
}
