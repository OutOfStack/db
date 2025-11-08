package network

// Client represents a generic database client interface
type Client interface {
	// Send sends data to the database server and returns the response
	Send(data []byte) ([]byte, error)
	// Close closes the client connection
	Close() error
}
