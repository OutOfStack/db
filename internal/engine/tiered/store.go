package tiered

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/OutOfStack/db/internal/wal"
)

// On-disk record layout (big-endian), append-only, one per mutation:
//
//	tableLen uint16 | keyLen uint16 | valLen uint32 | table | key | value | crc32
//
// valLen == tombstoneMarker marks a delete (no value bytes follow). The crc32
// covers the header and body, mirroring the WAL, so a torn tail from a crash is
// detected and truncated on recovery.
const (
	headerSize      = 8
	crcSize         = 4
	tombstoneMarker = 0xFFFFFFFF
	// maxFieldLen bounds table/key length (uint16 on disk).
	maxFieldLen = 1<<16 - 1
	// maxValueLen keeps valLen distinct from the tombstone marker.
	maxValueLen = tombstoneMarker - 1

	segPrefix = "seg-"
	segSuffix = ".data"
)

var (
	errPartial  = errors.New("partial tiered record")
	errChecksum = errors.New("tiered record checksum mismatch")
)

// decoded is one record read back from a segment.
type decoded struct {
	table     string
	key       string
	value     string
	tombstone bool
	recSize   int64
}

// valPosFor returns the absolute offset of a record's value bytes given the
// offset of the record start. The keydir stores this so a cache miss reads only
// the value, not the whole record.
func valPosFor(recPos int64, table, key string) int64 {
	return recPos + headerSize + int64(len(table)+len(key))
}

// u16/u32 convert lengths callers have already bounded: Set rejects table/key
// over maxFieldLen and values over maxValueLen, and lengths of decoded records
// come from on-disk uint16/uint32 fields. Centralized so the gosec overflow
// suppression lives in one place.
func u16(n int) uint16 { return uint16(n) } // #nosec G115 -- length bounded by maxFieldLen
func u32(n int) uint32 { return uint32(n) } // #nosec G115 -- length bounded by maxValueLen

func encodeRecord(table, key, value string, tombstone bool) []byte {
	valLen := u32(len(value))
	if tombstone {
		valLen = tombstoneMarker
	}
	size := headerSize + len(table) + len(key)
	if !tombstone {
		size += len(value)
	}
	buf := make([]byte, 0, size+crcSize)
	var hdr [headerSize]byte
	binary.BigEndian.PutUint16(hdr[0:2], u16(len(table)))
	binary.BigEndian.PutUint16(hdr[2:4], u16(len(key)))
	binary.BigEndian.PutUint32(hdr[4:8], valLen)
	buf = append(buf, hdr[:]...)
	buf = append(buf, table...)
	buf = append(buf, key...)
	if !tombstone {
		buf = append(buf, value...)
	}
	crc := crc32.ChecksumIEEE(buf)
	return binary.BigEndian.AppendUint32(buf, crc)
}

func decodeRecord(reader *bufio.Reader) (decoded, error) {
	hdr := make([]byte, headerSize)
	n, err := io.ReadFull(reader, hdr)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return decoded{}, io.EOF
		}
		return decoded{}, fmt.Errorf("%w: header: %w", errPartial, err)
	}
	tableLen := int(binary.BigEndian.Uint16(hdr[0:2]))
	keyLen := int(binary.BigEndian.Uint16(hdr[2:4]))
	valLen := binary.BigEndian.Uint32(hdr[4:8])
	tombstone := valLen == tombstoneMarker

	bodyLen := tableLen + keyLen
	if !tombstone {
		bodyLen += int(valLen)
	}
	body := make([]byte, bodyLen)
	if _, err = io.ReadFull(reader, body); err != nil {
		return decoded{}, fmt.Errorf("%w: body: %w", errPartial, err)
	}
	crcBytes := make([]byte, crcSize)
	if _, err = io.ReadFull(reader, crcBytes); err != nil {
		return decoded{}, fmt.Errorf("%w: checksum: %w", errPartial, err)
	}

	sum := crc32.NewIEEE()
	_, _ = sum.Write(hdr)
	_, _ = sum.Write(body)
	if sum.Sum32() != binary.BigEndian.Uint32(crcBytes) {
		return decoded{}, errChecksum
	}

	rec := decoded{
		table:     string(body[:tableLen]),
		key:       string(body[tableLen : tableLen+keyLen]),
		tombstone: tombstone,
		recSize:   int64(headerSize + bodyLen + crcSize),
	}
	if !tombstone {
		rec.value = string(body[tableLen+keyLen:])
	}
	return rec, nil
}

// store owns the append-only segment files. All methods are called under the
// engine mutex; it holds no lock of its own.
//
// readers and sizes are the only state: the active segment is just readers and
// sizes at activeSeg, so there is no shadow copy to keep in step.
type store struct {
	dir         string
	segmentSize int64
	sync        wal.SyncPolicy

	activeSeg uint32
	readers   map[uint32]*os.File // open handles per segment (includes active)
	sizes     map[uint32]int64    // bytes written per segment
}

func (s *store) active() *os.File { return s.readers[s.activeSeg] }

func (s *store) activeSize() int64 { return s.sizes[s.activeSeg] }

func openStore(dir string, segmentSize int64, sync wal.SyncPolicy) (*store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	s := &store{
		dir:         dir,
		segmentSize: segmentSize,
		sync:        sync,
		readers:     make(map[uint32]*os.File),
		sizes:       make(map[uint32]int64),
	}
	nums, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	for _, num := range nums {
		// #nosec G304 -- path built from the configured dir and a numeric segment id.
		file, openErr := os.OpenFile(filepath.Join(dir, segFilename(num)), os.O_RDWR, 0o600)
		if openErr != nil {
			return nil, fmt.Errorf("open segment %d: %w", num, openErr)
		}
		s.readers[num] = file
		info, statErr := file.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("stat segment %d: %w", num, statErr)
		}
		s.sizes[num] = info.Size()
	}
	if len(nums) > 0 {
		s.activeSeg = nums[len(nums)-1]
	} else if err = s.openNewActive(1); err != nil {
		return nil, err
	}
	return s, nil
}

// segments returns segment numbers in ascending order.
func (s *store) segments() []uint32 {
	nums := make([]uint32, 0, len(s.readers))
	for num := range s.readers {
		nums = append(nums, num)
	}
	slices.Sort(nums)
	return nums
}

func (s *store) openNewActive(num uint32) error {
	path := filepath.Join(s.dir, segFilename(num))
	// No O_APPEND: writes use WriteAt at an explicit offset, so the record
	// position never depends on the file's current seek offset (recovery leaves
	// it mid-file after buffered reads).
	// #nosec G304 -- path built from the configured dir and a numeric segment id.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create segment %d: %w", num, err)
	}
	s.activeSeg = num
	s.readers[num] = file
	s.sizes[num] = 0
	return nil
}

// append writes rec to the active segment (rotating first if it would overflow)
// and returns the segment number and the record's start offset.
func (s *store) append(rec []byte) (uint32, int64, error) {
	if size := s.activeSize(); size > 0 && size+int64(len(rec)) > s.segmentSize {
		if err := s.syncActive(); err != nil {
			return 0, 0, err
		}
		if err := s.openNewActive(s.activeSeg + 1); err != nil {
			return 0, 0, err
		}
	}
	recPos := s.activeSize()
	written, err := s.active().WriteAt(rec, recPos)
	s.sizes[s.activeSeg] += int64(written)
	if err != nil {
		return 0, 0, fmt.Errorf("write record: %w", err)
	}
	if written != len(rec) {
		return 0, 0, io.ErrShortWrite
	}
	return s.activeSeg, recPos, nil
}

// readValue reads valLen bytes at valPos from segment seg.
func (s *store) readValue(seg uint32, valPos int64, valLen uint32) (string, error) {
	file, ok := s.readers[seg]
	if !ok {
		return "", fmt.Errorf("segment %d not open", seg)
	}
	buf := make([]byte, valLen)
	if _, err := file.ReadAt(buf, valPos); err != nil {
		return "", fmt.Errorf("read value from segment %d: %w", seg, err)
	}
	return string(buf), nil
}

// scanBufSize buffers segment reads during recovery and compaction; the default
// bufio size would be one read syscall per 4 KiB of a multi-MiB segment.
const scanBufSize = 1 << 16

// scanSegment decodes seg from the start, invoking fn for each record with its
// start offset, and records the segment's real size. allowTornTail truncates a
// torn or checksum-invalid tail instead of failing — correct for the newest
// segment at recovery (it is a crash tail), never for a sealed one.
func (s *store) scanSegment(seg uint32, allowTornTail bool, fn func(rec decoded, recPos int64)) error {
	file := s.readers[seg]
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek segment %d: %w", seg, err)
	}
	reader := bufio.NewReaderSize(file, scanBufSize)
	var offset int64
	for {
		rec, err := decodeRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if allowTornTail && (errors.Is(err, errPartial) || errors.Is(err, errChecksum)) {
				if truncErr := file.Truncate(offset); truncErr != nil {
					return fmt.Errorf("truncate damaged tail: %w", truncErr)
				}
				break
			}
			return fmt.Errorf("corrupt segment %d at offset %d: %w", seg, offset, err)
		}
		fn(rec, offset)
		offset += rec.recSize
	}
	s.sizes[seg] = offset
	return nil
}

// removeSegment closes and deletes a segment (used by compaction).
func (s *store) removeSegment(seg uint32) error {
	if file, ok := s.readers[seg]; ok {
		if err := file.Close(); err != nil {
			return fmt.Errorf("close segment %d: %w", seg, err)
		}
		delete(s.readers, seg)
	}
	delete(s.sizes, seg)
	if err := os.Remove(filepath.Join(s.dir, segFilename(seg))); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove segment %d: %w", seg, err)
	}
	return nil
}

// diskBytes is the total size of every segment, and len(sizes) their count.
func (s *store) diskBytes() int64 {
	var total int64
	for _, size := range s.sizes {
		total += size
	}
	return total
}

func (s *store) syncActive() error {
	file := s.active()
	if s.sync == wal.SyncNo || file == nil {
		return nil
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync segment: %w", err)
	}
	return nil
}

// syncIfAlways fsyncs the active segment only under the SyncAlways policy; the
// per-write durability path calls it, while SyncEverySec relies on the ticker.
func (s *store) syncIfAlways() error {
	if s.sync != wal.SyncAlways {
		return nil
	}
	return s.syncActive()
}

func (s *store) close() error {
	firstErr := s.syncActive()
	for _, file := range s.readers {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close segment: %w", err)
		}
	}
	return firstErr
}

func segFilename(num uint32) string {
	return fmt.Sprintf("%s%010d%s", segPrefix, num, segSuffix)
}

func listSegments(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read data directory: %w", err)
	}
	var nums []uint32
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		text := strings.TrimSuffix(strings.TrimPrefix(name, segPrefix), segSuffix)
		num, parseErr := strconv.ParseUint(text, 10, 32)
		if parseErr != nil {
			continue
		}
		nums = append(nums, uint32(num))
	}
	slices.Sort(nums)
	return nums, nil
}
