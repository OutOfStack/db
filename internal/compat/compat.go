// Package compat holds the v1 golden fixtures and the tests that read them. It has no code of its own: the bytes under
// testdata/golden/v1 and the expectations beside them are the compatibility contract.
//
// The fixtures were generated once, at v1.0, from the formats of that release: a snapshot and a WAL segment the
// recovery path must still replay to the same state, and recorded request/reply byte streams the codec must still
// produce and decode byte for byte. Every build runs the reader tests, so a change that would silently alter a format
// fails here rather than in a user's data directory.
//
// v1.x may add a fixture set for a newly frozen shape; it must never regenerate v1.0's. Regenerating is how a format
// change gets rubber-stamped instead of caught — the generator exists only for the next major version, and is guarded
// behind a flag for that reason.
package compat
