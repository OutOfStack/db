package client

import (
	"errors"
	"strings"

	"github.com/OutOfStack/db/internal/network"
)

// Wire error codes carried by ServerError.Code. Every error reply starts with one of these, and the set is frozen for
// v1.x: a code keeps its meaning, and a condition that reports CodeErr today may be given a narrower code later. The
// client reports a code it does not know — one a later server introduced — as CodeErr, so code written against this
// version keeps matching the conditions it matched before.
//
// They are spelled out here rather than aliased from an internal package so the public API documents its own strings;
// the error-code matrix test drives a real server for each one, which is what keeps them from drifting.
const (
	// CodeErr is the unclassified failure.
	CodeErr = "ERR"
	// CodeProtocol reports a malformed request frame; the server closes the connection after replying.
	CodeProtocol = "PROTOCOL"
	// CodeUnknownCommand reports a command name the server does not implement.
	CodeUnknownCommand = "UNKNOWNCMD"
	// CodeArity reports a known command given the wrong number of arguments.
	CodeArity = "ARITY"
	// CodeArgument reports an argument the command cannot interpret.
	CodeArgument = "ARGUMENT"
	// CodeTooLarge reports input or output past a configured or format limit.
	CodeTooLarge = "TOOLARGE"
	// CodeWrongType reports an operation applied to a key of another type, or arithmetic the type cannot represent.
	// ErrWrongType matches it.
	CodeWrongType = "WRONGTYPE"
	// CodeReadOnly reports a mutation sent to a replication standby. A pooled client re-routes it to a master on its own.
	CodeReadOnly = "READONLY"
	// CodeUnavailable reports a command the server cannot serve in its current state.
	CodeUnavailable = "UNAVAILABLE"
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

// ErrWrongType reports an operation applied to a key holding another type (Incr on a string, HGet on an array), or
// arithmetic the stored type cannot represent. It matches every *ServerError whose Code is CodeWrongType, so
// errors.Is(err, ErrWrongType) is the way to branch on it; the message names the types involved.
//
// It is one of only three exported sentinels — with ErrNotFound and ErrOutcomeUnknown — because it is one of only three
// conditions a caller usually handles differently rather than reports. Branch on ServerError.Code for the rest.
var ErrWrongType = errors.New("wrong type")

// ServerError represents an error message returned by the server. Code is the stable wire error code, always one of
// the Code constants: a code this version of the client does not recognize is reported as CodeErr, with the server's
// token kept at the start of Msg. Msg is the human-readable text, which is not part of the compatibility promise and may
// change between releases.
type ServerError struct {
	Code string
	Msg  string
}

// Error implements the error interface
func (e *ServerError) Error() string {
	if e.Code == "" || e.Code == CodeErr {
		return "server error: " + e.Msg
	}
	return "server error: " + e.Code + ": " + e.Msg
}

// Is lets errors.Is match a coded server error against the exported sentinel for that code.
func (e *ServerError) Is(target error) bool {
	return target == ErrWrongType && e.Code == CodeWrongType
}

// knownCode reports code when this client recognizes it, and CodeErr otherwise.
func knownCode(code string) (string, bool) {
	switch code {
	case CodeErr, CodeProtocol, CodeUnknownCommand, CodeArity, CodeArgument, CodeTooLarge, CodeWrongType, CodeReadOnly,
		CodeUnavailable:
		return code, true
	default:
		return CodeErr, false
	}
}

// serverError builds the error for a decoded error reply. An unrecognized code degrades to CodeErr, as the contract
// requires, and its token moves into the message so no information is lost.
func serverError(code, msg string) *ServerError {
	known, ok := knownCode(code)
	if !ok && code != "" {
		msg = strings.TrimSpace(code + " " + msg)
	}
	return &ServerError{Code: known, Msg: msg}
}
