package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/OutOfStack/db/internal/protocol"
)

const (
	checksumSize  = 4
	lsnSize       = 8
	maxRecordSize = 64 << 20

	// CommandSet and CommandDel are the mutating operations accepted by the WAL.
	CommandSet = "SET"
	CommandDel = "DEL"
)

var (
	// ErrChecksum is returned when a WAL record does not match its checksum.
	ErrChecksum = errors.New("wal checksum mismatch")
	// ErrPartialRecord is returned when the end of a record is missing.
	ErrPartialRecord = errors.New("partial wal record")
)

// Record is one mutation stored in the write-ahead log.
type Record struct {
	LSN     uint64
	Command string
	Args    []string
}

func encodeRecord(record Record) ([]byte, error) {
	var body bytes.Buffer
	if err := binary.Write(&body, binary.BigEndian, record.LSN); err != nil {
		return nil, fmt.Errorf("encode LSN: %w", err)
	}
	if err := protocol.WriteCommand(&body, record.Command, record.Args); err != nil {
		return nil, fmt.Errorf("encode command: %w", err)
	}

	encoded := body.Bytes()
	checksum := crc32.ChecksumIEEE(encoded)
	result := make([]byte, 0, len(encoded)+checksumSize)
	result = append(result, encoded...)
	result = binary.BigEndian.AppendUint32(result, checksum)
	return result, nil
}

func readRecord(reader *bufio.Reader) (Record, error) {
	lsnBytes := make([]byte, lsnSize)
	n, err := io.ReadFull(reader, lsnBytes)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return Record{}, io.EOF
		}
		return Record{}, fmt.Errorf("%w: read LSN: %w", ErrPartialRecord, err)
	}

	command, args, err := protocol.ReadCommand(reader, maxRecordSize)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, fmt.Errorf("%w: read command: %w", ErrPartialRecord, err)
		}
		return Record{}, fmt.Errorf("decode command: %w", err)
	}

	checksumBytes := make([]byte, checksumSize)
	if _, err = io.ReadFull(reader, checksumBytes); err != nil {
		return Record{}, fmt.Errorf("%w: read checksum: %w", ErrPartialRecord, err)
	}

	record := Record{LSN: binary.BigEndian.Uint64(lsnBytes), Command: command, Args: args}
	encoded, err := encodeRecord(record)
	if err != nil {
		return Record{}, err
	}
	want := binary.BigEndian.Uint32(checksumBytes)
	got := binary.BigEndian.Uint32(encoded[len(encoded)-checksumSize:])
	if got != want {
		return Record{}, fmt.Errorf("%w: got %08x, want %08x", ErrChecksum, got, want)
	}
	if err = validateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func validateRecord(record Record) error {
	switch record.Command {
	case CommandSet:
		if len(record.Args) != 3 {
			return fmt.Errorf("invalid SET WAL record: got %d arguments", len(record.Args))
		}
	case CommandDel:
		if len(record.Args) != 2 {
			return fmt.Errorf("invalid DEL WAL record: got %d arguments", len(record.Args))
		}
	default:
		return fmt.Errorf("invalid WAL record command %q", record.Command)
	}
	return nil
}
