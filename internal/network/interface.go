package network

import "github.com/OutOfStack/db/internal/protocol"

// Client represents a generic database client interface
type Client interface {
	// Send sends a command to the database server and returns the response.
	Send(cmd string, args []string) (protocol.Reply, error)
	// Close closes the client connection
	Close() error
}
