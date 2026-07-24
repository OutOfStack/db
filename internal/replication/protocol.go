// Package replication implements master/standby log shipping. The master streams
// its write-ahead log to standbys, which persist and apply it in order and serve
// reads. Replication is asynchronous: the master acknowledges clients without
// waiting for standbys. Failover is manual via the PROMOTE admin command.
//
// The wire protocol runs on a dedicated master listener, separate from the
// client-facing TCP server. A standby opens a connection and sends a single
// RESP command handshake:
//
//	REPLICATE <lsn>
//
// where <lsn> is the highest LSN the standby has already applied. The master
// then streams framed messages, each prefixed by a one-byte frame type:
//
//	'R' record    — a WAL record (wal.EncodeRecord bytes; self-delimiting)
//	'S' snapshot  — resync payload: 8-byte LSN, 8-byte length, then that many
//	                bytes of protocol-encoded SET commands (a snapshot blob)
//	'H' heartbeat — 8-byte master LastLSN, sent while idle so the standby can
//	                report replication lag even when no records are flowing
package replication

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/wal"
)

const (
	// handshakeCommand is the RESP command a standby sends to begin streaming.
	handshakeCommand = "REPLICATE"

	frameRecord    byte = 'R'
	frameSnapshot  byte = 'S'
	frameHeartbeat byte = 'H'

	// handshakeMaxSize bounds the REPLICATE handshake command decode.
	handshakeMaxSize = 128
)

// writeHandshake sends the REPLICATE <lsn> handshake to the master.
func writeHandshake(w io.Writer, appliedLSN uint64) error {
	return protocol.WriteCommand(w, handshakeCommand, []string{strconv.FormatUint(appliedLSN, 10)})
}

// readHandshake reads and validates a REPLICATE <lsn> handshake.
func readHandshake(r *bufio.Reader) (uint64, error) {
	cmd, args, err := protocol.ReadCommand(r, handshakeMaxSize)
	if err != nil {
		return 0, fmt.Errorf("read handshake: %w", err)
	}
	if cmd != handshakeCommand || len(args) != 1 {
		return 0, fmt.Errorf("unexpected handshake %q with %d args", cmd, len(args))
	}
	lsn, err := parseUint(args[0])
	if err != nil {
		return 0, fmt.Errorf("invalid handshake LSN %q: %w", args[0], err)
	}
	return lsn, nil
}

func writeRecordFrame(w io.Writer, record wal.Record) error {
	encoded, err := wal.EncodeRecord(record)
	if err != nil {
		return err
	}
	if _, err = w.Write([]byte{frameRecord}); err != nil {
		return err
	}
	_, err = w.Write(encoded)
	return err
}

func writeHeartbeatFrame(w io.Writer, lastLSN uint64) error {
	buf := make([]byte, 1+8)
	buf[0] = frameHeartbeat
	binary.BigEndian.PutUint64(buf[1:], lastLSN)
	_, err := w.Write(buf)
	return err
}

// writeSnapshotFrame streams a snapshot blob of the given byte length. The body
// is copied from src (typically a snapshot file) after the header.
func writeSnapshotFrame(w io.Writer, lsn uint64, length int64, src io.Reader) error {
	if length < 0 {
		return fmt.Errorf("negative snapshot length %d", length)
	}
	header := make([]byte, 1+8+8)
	header[0] = frameSnapshot
	binary.BigEndian.PutUint64(header[1:9], lsn)
	binary.BigEndian.PutUint64(header[9:17], uint64(length)) // #nosec G115 -- length guarded non-negative above
	if _, err := w.Write(header); err != nil {
		return err
	}
	copied, err := io.CopyN(w, src, length)
	if err != nil {
		return fmt.Errorf("stream snapshot body: %w", err)
	}
	if copied != length {
		return fmt.Errorf("short snapshot stream: wrote %d of %d bytes", copied, length)
	}
	return nil
}

func parseUint(s string) (uint64, error) {
	var n uint64
	if s == "" {
		return 0, errors.New("empty number")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-digit %q", r)
		}
		n = n*10 + uint64(r-'0')
	}
	return n, nil
}
