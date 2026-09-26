package protocol

import (
	"errors"
	"fmt"
	"strings"
)

// Wire error codes. Every error reply starts with one of these tokens followed by a space and a human-readable message,
// so a client can branch on the code without matching message text. The set is part of the v1 contract: codes are never
// removed or given a new meaning within v1.x, and a new code is only ever introduced for a condition that previously
// reported CodeErr.
const (
	// CodeErr is the unclassified failure. A client that does not recognize a code should treat it as this one.
	CodeErr = "ERR"
	// CodeProtocol reports a malformed RESP frame. The connection is closed after the reply.
	CodeProtocol = "PROTOCOL"
	// CodeUnknownCommand reports a command name the server does not implement.
	CodeUnknownCommand = "UNKNOWNCMD"
	// CodeArity reports a known command given the wrong number of arguments.
	CodeArity = "ARITY"
	// CodeArgument reports an argument the command cannot interpret: an empty table or key, an unparsable value literal,
	// a non-numeric INCR delta.
	CodeArgument = "ARGUMENT"
	// CodeTooLarge reports input or output that exceeds a configured or format limit.
	CodeTooLarge = "TOOLARGE"
	// CodeWrongType reports an operation applied to a key holding another type, and value arithmetic the type cannot
	// represent (overflow, non-finite results, nesting past the codec's depth).
	CodeWrongType = "WRONGTYPE"
	// CodeReadOnly reports a mutation sent to a replication standby. A pooled client re-routes it to a master.
	CodeReadOnly = "READONLY"
	// CodeUnavailable reports a command the server cannot serve in its current state: storage fenced into a terminal
	// state, or a feature this node does not run.
	CodeUnavailable = "UNAVAILABLE"
)

// Coded is implemented by errors that carry a wire error code. Layers below the wire return coded errors so the code is
// decided where the failure is understood, rather than by matching message text at the edge.
type Coded interface {
	error
	ErrorCode() string
}

// CommandError is an error carrying a wire error code.
type CommandError struct {
	Code string
	Msg  string
}

// NewError builds a coded error. The message must not repeat the code: WriteReply emits both.
func NewError(code, format string, args ...any) *CommandError {
	return &CommandError{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// Error implements the error interface.
func (e *CommandError) Error() string { return e.Msg }

// ErrorCode implements Coded.
func (e *CommandError) ErrorCode() string { return e.Code }

// CodeOf reports the wire error code for err, walking wrapped errors. An error that carries no code is CodeErr.
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	if coded, ok := errors.AsType[Coded](err); ok {
		return coded.ErrorCode()
	}
	return CodeErr
}

// splitErrorCode separates the leading code token of a decoded error reply from its message. A reply whose first token
// is not code-shaped is reported as CodeErr with the whole line as the message, so a server older than this contract
// still decodes to something usable.
func splitErrorCode(line string) (code, msg string) {
	token, rest, _ := strings.Cut(line, " ")
	if !isErrorCode(token) {
		return CodeErr, line
	}
	return token, rest
}

// isErrorCode reports whether token has the shape reserved for codes: a non-empty run of upper-case ASCII letters.
func isErrorCode(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}
