package client

// Transport is a test-only alias for the internal transport interface
type Transport = transport

// NewWithTransport is a test-only constructor that injects a custom transport
func NewWithTransport(t Transport) *Client {
	return &Client{transport: t}
}
