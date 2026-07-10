package client

import "errors"

// ErrNotFound is returned by Get and Del when the key does not exist
var ErrNotFound = errors.New("not found")

// ServerError represents an error message returned by the server
type ServerError struct {
	Msg string
}

// Error implements the error interface
func (e *ServerError) Error() string {
	return "server error: " + e.Msg
}
