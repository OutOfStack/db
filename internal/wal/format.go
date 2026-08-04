package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// Format headers identify the files this package writes; the trailing byte is the format version. A non-empty file that
// does not carry the current header is rejected at open rather than parsed: misreading one would look like a torn tail
// and get truncated away.
const (
	walHeader      = "DBWAL\x00\x02"
	snapshotHeader = "DBSNP\x00\x02"
)

// ErrUnsupportedFormat is returned for a file written in a format this build does not read.
var ErrUnsupportedFormat = errors.New("unsupported file format")

// RequireHeader verifies that file starts with header and returns the offset its records begin at. An empty file is
// accepted as one with no records: a crash between creating a segment and writing its header leaves exactly that.
// The tiered engine shares it (and WriteHeader) so both packages enforce the same format-header contract.
func RequireHeader(file *os.File, header string) (int64, error) {
	buf := make([]byte, len(header))
	n, err := file.ReadAt(buf, 0)
	if n == 0 && errors.Is(err, io.EOF) {
		return 0, nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if string(buf[:n]) != header {
		return 0, ErrUnsupportedFormat
	}
	return int64(len(header)), nil
}

// consumeHeader verifies and skips the header on a buffered reader.
func consumeHeader(reader *bufio.Reader, header string) error {
	buf, err := reader.Peek(len(header))
	if len(buf) == 0 && errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if string(buf) != header {
		return ErrUnsupportedFormat
	}
	_, err = reader.Discard(len(header))
	return err
}

// WriteHeader writes a format header at the start of a freshly opened, empty file, treating a short write as an error.
func WriteHeader(file *os.File, header string) error {
	written, err := file.WriteString(header)
	if err == nil && written != len(header) {
		err = io.ErrShortWrite
	}
	return err
}

// openTailSegment opens the newest segment for appending and returns it with the size to append at. An empty file is a
// crash between creating a segment and writing its header: the header is written now, since records appended into it
// would otherwise sit where the header belongs and the next open would reject the whole segment.
func openTailSegment(path string) (*os.File, int64, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600) // #nosec G304 -- path comes from the WAL directory listing
	if err != nil {
		return nil, 0, fmt.Errorf("open WAL segment: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, 0, fmt.Errorf("stat WAL segment: %w", err)
	}
	if size := info.Size(); size > 0 {
		if _, err = RequireHeader(file, walHeader); err != nil {
			_ = file.Close()
			return nil, 0, fmt.Errorf("read WAL segment header: %w", err)
		}
		return file, size, nil
	}
	if err = WriteHeader(file, walHeader); err != nil {
		_ = file.Close()
		return nil, 0, fmt.Errorf("write WAL segment header: %w", err)
	}
	return file, int64(len(walHeader)), nil
}

func openWALSegment(path string) (*os.File, *bufio.Reader, error) {
	file, err := os.Open(path) // #nosec G304 -- path comes from the WAL directory listing
	if err != nil {
		return nil, nil, err
	}
	offset, err := RequireHeader(file, walHeader)
	if err == nil {
		_, err = file.Seek(offset, io.SeekStart)
	}
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("read WAL segment header: %w", err)
	}
	return file, bufio.NewReader(file), nil
}
