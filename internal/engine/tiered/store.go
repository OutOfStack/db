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
	stdsync "sync"

	"github.com/OutOfStack/db/internal/wal"
)

// On-disk record layout (big-endian), append-only, one per mutation:
//
//	tableLen uint16 | keyLen uint16 | valLen uint32 | table | key | value | crc32
//
// valLen == tombstoneMarker marks a delete (no value bytes follow). The crc32 covers the header and body, mirroring the
// WAL, so a torn tail from a crash is detected and truncated on recovery.
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

// segmentHeader identifies a segment written by this build; the trailing byte is the format version. A non-empty
// segment without it is rejected at open rather than parsed, since misreading one looks like a torn tail and gets
// truncated away.
const segmentHeader = "DBSEG\x00\x02"

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

// valPosFor returns the absolute offset of a record's value bytes given the offset of the record start. The keydir
// stores this so a cache miss reads only the value, not the whole record.
func valPosFor(recPos int64, table, key string) int64 {
	return recPos + headerSize + int64(len(table)+len(key))
}

// u16/u32 convert lengths callers have already bounded: Set rejects table/key over maxFieldLen and values over
// maxValueLen, and lengths of decoded records come from on-disk uint16/uint32 fields. Centralized so the gosec overflow
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

// store owns the append-only segment files.
//
// Its maps are guarded by the engine lock: readers hold it at least shared, and the engine takes it exclusively to add
// or remove a segment. pinMu is a second, narrower lock covering only the pin counts and the handle lookup that hands
// one out. The two exist because a pinned segment is read with the engine lock released, so removeSegment waits for the
// pin to drop rather than closing the file underneath a reader.
//
// The active segment is just readers and sizes at activeSeg, so there is no shadow copy to keep in step.
type store struct {
	dir         string
	segmentSize int64
	sync        wal.SyncPolicy

	activeSeg uint32
	readers   map[uint32]*os.File // open handles per segment (includes active)
	sizes     map[uint32]int64    // bytes written per segment

	pinMu stdsync.Mutex
	pins  map[uint32]int
	cond  *stdsync.Cond
}

func (s *store) active() *os.File { return s.readers[s.activeSeg] }

func (s *store) activeSize() int64 { return s.sizes[s.activeSeg] }

// dataSize is a segment's record bytes, excluding its format header.
func (s *store) dataSize(seg uint32) int64 {
	return max(0, s.sizes[seg]-int64(len(segmentHeader)))
}

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
		pins:        make(map[uint32]int),
	}
	s.cond = stdsync.NewCond(&s.pinMu)
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
		size := info.Size()
		if size == 0 {
			// A crash between creating a segment and writing its header leaves an empty file; finish the job rather than
			// rejecting it as unreadable.
			if err = wal.WriteHeader(file, segmentHeader); err != nil {
				return nil, fmt.Errorf("write segment %d header: %w", num, err)
			}
			size = int64(len(segmentHeader))
		} else if _, err = wal.RequireHeader(file, segmentHeader); err != nil {
			return nil, fmt.Errorf("segment %d: %w", num, err)
		}
		s.sizes[num] = size
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
	// No O_APPEND: writes use WriteAt at an explicit offset, so the record position never depends on the file's current
	// seek offset (recovery leaves it mid-file after buffered reads).
	// #nosec G304 -- path built from the configured dir and a numeric segment id.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create segment %d: %w", num, err)
	}
	if err = wal.WriteHeader(file, segmentHeader); err != nil {
		_ = file.Close()
		return fmt.Errorf("write segment header: %w", err)
	}
	if s.sync != wal.SyncNo {
		if err = file.Sync(); err != nil {
			_ = file.Close()
			return fmt.Errorf("sync segment header: %w", err)
		}
	}
	// Syncing the file alone does not persist its directory entry, so a crash right after rotation could lose the whole
	// segment along with writes that were already acknowledged.
	if s.sync != wal.SyncNo {
		if err = wal.SyncDirectory(s.dir); err != nil {
			return fmt.Errorf("sync data directory: %w", err)
		}
	}
	s.activeSeg = num
	s.readers[num] = file
	s.sizes[num] = int64(len(segmentHeader))
	return nil
}

// append writes rec to the active segment (rotating first if it would overflow) and returns the segment number and the
// record's start offset.
func (s *store) append(rec []byte) (uint32, int64, error) {
	if dataSize := s.dataSize(s.activeSeg); dataSize > 0 && dataSize+int64(len(rec)) > s.segmentSize {
		if err := s.syncActive(); err != nil {
			return 0, 0, err
		}
		if err := s.openNewActive(s.activeSeg + 1); err != nil {
			return 0, 0, err
		}
	}
	recPos := s.activeSize()
	written, err := s.active().WriteAt(rec, recPos)
	if err == nil && written != len(rec) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// Drop the partial record instead of counting it: leaving it in place would put later (acknowledged) appends behind
		// damage that recovery truncates away.
		if truncErr := s.active().Truncate(recPos); truncErr != nil {
			return 0, 0, errors.Join(fmt.Errorf("write record: %w", err), truncErr)
		}
		return 0, 0, fmt.Errorf("write record: %w", err)
	}
	s.sizes[s.activeSeg] += int64(written)
	return s.activeSeg, recPos, nil
}

// readValue reads valLen bytes at valPos from segment seg.
func (s *store) readValue(seg uint32, valPos int64, valLen uint32) (string, error) {
	file, ok := s.pin(seg)
	if !ok {
		return "", fmt.Errorf("segment %d not open", seg)
	}
	defer s.unpin(seg)
	return readPinnedValue(seg, file, valPos, valLen)
}

func readPinnedValue(seg uint32, file *os.File, valPos int64, valLen uint32) (string, error) {
	buf := make([]byte, valLen)
	if _, err := file.ReadAt(buf, valPos); err != nil {
		return "", fmt.Errorf("read value from segment %d: %w", seg, err)
	}
	return string(buf), nil
}

// scanBufSize buffers segment reads during recovery and compaction; the default bufio size would be one read syscall
// per 4 KiB of a multi-MiB segment.
const scanBufSize = 1 << 16

// scanSegment decodes seg from the start, invoking fn for each record with its start offset, and records the segment's
// real size. allowTornTail truncates a torn or checksum-invalid tail instead of failing — correct for the newest
// segment at recovery (it is a crash tail), never for a sealed one.
func (s *store) scanSegment(seg uint32, allowTornTail bool, fn func(rec decoded, recPos int64)) error {
	file, ok := s.pin(seg)
	if !ok {
		return fmt.Errorf("segment %d not open", seg)
	}
	defer s.unpin(seg)
	offset, err := scanPinnedSegment(seg, file, allowTornTail, fn)
	if err != nil {
		return err
	}
	s.sizes[seg] = offset
	return nil
}

// scanPinnedSegment decodes a pinned segment from its first record and returns the offset it stopped at. It seeks the
// shared handle, so only one scan may run on a segment at a time: recovery scans before the engine serves anything, and
// compaction is serialized by the compacting flag. Value reads are unaffected — they use ReadAt, which ignores the file
// offset.
func scanPinnedSegment(seg uint32, file *os.File, allowTornTail bool, fn func(rec decoded, recPos int64)) (int64, error) {
	offset := int64(len(segmentHeader))
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek segment %d: %w", seg, err)
	}
	reader := bufio.NewReaderSize(file, scanBufSize)
	for {
		rec, err := decodeRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if allowTornTail && (errors.Is(err, errPartial) || errors.Is(err, errChecksum)) {
				if truncErr := file.Truncate(offset); truncErr != nil {
					return 0, fmt.Errorf("truncate damaged tail: %w", truncErr)
				}
				break
			}
			return 0, fmt.Errorf("corrupt segment %d at offset %d: %w", seg, offset, err)
		}
		fn(rec, offset)
		offset += rec.recSize
	}
	return offset, nil
}

// removeSegment closes and deletes a segment (used by compaction).
func (s *store) removeSegment(seg uint32) error {
	s.pinMu.Lock()
	for s.pins[seg] > 0 {
		s.cond.Wait()
	}
	if file, ok := s.readers[seg]; ok {
		if err := file.Close(); err != nil {
			s.pinMu.Unlock()
			return fmt.Errorf("close segment %d: %w", seg, err)
		}
		delete(s.readers, seg)
	}
	s.pinMu.Unlock()
	delete(s.sizes, seg)
	if err := os.Remove(filepath.Join(s.dir, segFilename(seg))); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove segment %d: %w", seg, err)
	}
	return nil
}

// pin hands out a segment's handle and holds it open: the caller reads from it with the engine lock released, and
// removeSegment waits for the pin to drop.
func (s *store) pin(seg uint32) (*os.File, bool) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	file, ok := s.readers[seg]
	if !ok {
		return nil, false
	}
	s.pins[seg]++
	return file, true
}

func (s *store) unpin(seg uint32) {
	s.pinMu.Lock()
	s.pins[seg]--
	if s.pins[seg] == 0 {
		delete(s.pins, seg)
		s.cond.Broadcast()
	}
	s.pinMu.Unlock()
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

// syncIfAlways fsyncs the active segment only under the SyncAlways policy; the per-write durability path calls it,
// while SyncEverySec relies on the ticker.
func (s *store) syncIfAlways() error {
	if s.sync != wal.SyncAlways {
		return nil
	}
	return s.syncActive()
}

func (s *store) close() error {
	s.pinMu.Lock()
	for len(s.pins) > 0 {
		s.cond.Wait()
	}
	defer s.pinMu.Unlock()
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
