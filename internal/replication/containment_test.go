package replication_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/engine"
	"github.com/OutOfStack/db/internal/protocol"
	"github.com/OutOfStack/db/internal/replication"
	"github.com/OutOfStack/db/internal/storage"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

// fakeMaster is a listener the tests script by hand, so they can send exactly the frames they want a standby to see.
type fakeMaster struct {
	t        *testing.T
	listener net.Listener
}

func newFakeMaster(t *testing.T) *fakeMaster {
	t.Helper()
	lc := net.ListenConfig{}
	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return &fakeMaster{t: t, listener: listener}
}

func (f *fakeMaster) addr() string { return f.listener.Addr().String() }

// accept waits for a standby, reads its handshake, and returns the connection and the LSN it asked to resume from.
func (f *fakeMaster) accept() (net.Conn, uint64) {
	f.t.Helper()
	conn, err := f.listener.Accept()
	require.NoError(f.t, err)
	require.NoError(f.t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	cmd, args, err := protocol.ReadCommand(bufio.NewReader(conn), 128)
	require.NoError(f.t, err)
	require.Equal(f.t, "REPLICATE", cmd)
	require.Len(f.t, args, 1)
	var lsn uint64
	for _, r := range args[0] {
		lsn = lsn*10 + uint64(r-'0')
	}
	return conn, lsn
}

// snapshotLSN is the LSN of the snapshot snapshotBytes produces.
const snapshotLSN = 3

func snapshotFrameHeader(length int) []byte {
	header := make([]byte, 1+8+8)
	header[0] = 'S'
	binary.BigEndian.PutUint64(header[1:9], snapshotLSN)
	binary.BigEndian.PutUint64(header[9:17], uint64(length)) // #nosec G115 -- test lengths are small
	return header
}

func heartbeatFrame(lsn uint64) []byte {
	frame := make([]byte, 9)
	frame[0] = 'H'
	binary.BigEndian.PutUint64(frame[1:], lsn)
	return frame
}

// snapshotBytes writes a three-entry snapshot at LSN 3 through a real node and returns the file's bytes.
func snapshotBytes(t *testing.T) []byte {
	t.Helper()
	n := newNode(t, t.TempDir())
	set(t, n, "t", "a", "1")
	set(t, n, "t", "b", "2")
	set(t, n, "t", "c", "3")
	snapshot(t, n)
	_, path, ok, err := wal.LatestSnapshotInfo(n.dir)
	require.NoError(t, err)
	require.True(t, ok)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func requireFenced(t *testing.T, n *node) {
	t.Helper()
	_, err := n.store.Execute(context.Background(), "GET", []string{"t", "a"})
	require.ErrorIs(t, err, storage.ErrTerminal)
	_, err = n.store.Execute(context.Background(), "TABLES", nil)
	require.ErrorIs(t, err, storage.ErrTerminal)
	status := n.store.Status()
	require.False(t, status.Ready)
	require.True(t, status.Degraded)
	require.ErrorIs(t, status.TerminalError, storage.ErrTerminal)
}

// TestStandbyRejectsSnapshotTruncatedAtEveryOffset cuts a resync frame short at every byte offset — every record
// boundary included — and verifies the standby never applies it, never advances, and stays healthy enough to accept
// the complete snapshot afterwards.
func TestStandbyRejectsSnapshotTruncatedAtEveryOffset(t *testing.T) {
	t.Parallel()
	data := snapshotBytes(t)
	master := newFakeMaster(t)
	standby := newNode(t, t.TempDir())
	sb := replication.NewStandby(master.addr(), standby.store, standby.dir, 0, time.Millisecond, nil)
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)

	for cut := range data {
		conn, lsn := master.accept()
		require.Zero(t, lsn, "standby advanced after a truncated snapshot (cut at %d)", cut)
		_, err := conn.Write(snapshotFrameHeader(len(data)))
		require.NoError(t, err)
		_, err = conn.Write(data[:cut])
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	require.Zero(t, sb.AppliedLSN())
	require.NoError(t, sb.Terminal(), "a short frame changes nothing, so it is retried rather than terminal")
	_, err := standby.engine.Get(context.Background(), "t", "a")
	require.ErrorIs(t, err, engine.ErrNotFound)

	conn, _ := master.accept()
	_, err = conn.Write(snapshotFrameHeader(len(data)))
	require.NoError(t, err)
	_, err = conn.Write(data)
	require.NoError(t, err)
	waitFor(t, "complete snapshot to apply", func() bool { return sb.AppliedLSN() == 3 })
	require.Equal(t, "3", mustGet(t, standby, "t", "c"))
	require.NoError(t, conn.Close())
}

// TestStandbyRefusesOversizedSnapshotBeforeReading declares a frame over the configured byte cap: the standby drops
// the connection without reading the body, and a snapshot over the entry cap is rejected before any state changes.
func TestStandbyRefusesOversizedSnapshotBeforeReading(t *testing.T) {
	t.Parallel()
	data := snapshotBytes(t)
	master := newFakeMaster(t)
	standby := newNode(t, t.TempDir())
	sb := replication.NewStandby(master.addr(), standby.store, standby.dir, 0, time.Millisecond, nil,
		replication.WithMaxSnapshotBytes(int64(len(data))), replication.WithMaxSnapshotEntries(2))
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)

	conn, _ := master.accept()
	_, err := conn.Write(snapshotFrameHeader(len(data) + 1))
	require.NoError(t, err)
	// The standby hangs up on the declared length alone; it never asks for the body.
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	require.NoError(t, conn.Close())

	conn, _ = master.accept()
	_, err = conn.Write(snapshotFrameHeader(len(data)))
	require.NoError(t, err)
	_, err = conn.Write(data)
	require.NoError(t, err)
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "standby should drop a snapshot over the entry cap")
	require.NoError(t, conn.Close())

	require.Zero(t, sb.AppliedLSN())
	require.NoError(t, sb.Terminal())
	require.True(t, standby.store.Status().Ready)
	_, err = standby.engine.Get(context.Background(), "t", "a")
	require.ErrorIs(t, err, engine.ErrNotFound)
}

// failingApplier records Fence calls and fails ResetToSnapshot, standing in for a resync interrupted after it began
// changing state.
type failingApplier struct {
	fenced chan error
}

func (f *failingApplier) ApplyReplicated(context.Context, wal.Record) error { return nil }
func (f *failingApplier) ResetToSnapshot(context.Context, string, uint64, []engine.Entry) error {
	return errors.New("disk vanished")
}
func (f *failingApplier) Fence(err error) { f.fenced <- err }

// TestFailedResyncIsTerminal verifies a resync that fails once the standby has begun replacing its state fences the
// storage, stops the replication loop for good, and reports why.
func TestFailedResyncIsTerminal(t *testing.T) {
	t.Parallel()
	data := snapshotBytes(t)
	master := newFakeMaster(t)
	applier := &failingApplier{fenced: make(chan error, 1)}
	sb := replication.NewStandby(master.addr(), applier, t.TempDir(), 0, time.Millisecond, nil)
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)

	conn, _ := master.accept()
	_, err := conn.Write(snapshotFrameHeader(len(data)))
	require.NoError(t, err)
	_, err = conn.Write(data)
	require.NoError(t, err)
	select {
	case fenceErr := <-applier.fenced:
		require.ErrorIs(t, fenceErr, replication.ErrTerminal)
		require.ErrorContains(t, fenceErr, "disk vanished")
		require.ErrorContains(t, fenceErr, "restart the standby")
	case <-time.After(5 * time.Second):
		t.Fatal("storage was never fenced")
	}
	require.ErrorIs(t, sb.Terminal(), replication.ErrTerminal)
	require.Zero(t, sb.AppliedLSN())
	require.NoError(t, conn.Close())
	// The loop has stopped: no reconnect arrives.
	tcpListener, ok := master.listener.(*net.TCPListener)
	require.True(t, ok)
	require.NoError(t, tcpListener.SetDeadline(time.Now().Add(200*time.Millisecond)))
	_, err = tcpListener.Accept()
	require.Error(t, err, "terminal standby reconnected")
}

// TestResyncCancelledMidApplyIsTerminalUntilRestart interrupts a real resync while the storage is replacing its state.
// The running standby fences itself; a restart recovers from disk and reseeds cleanly from the master.
func TestResyncCancelledMidApplyIsTerminalUntilRestart(t *testing.T) {
	t.Parallel()
	master := newNode(t, t.TempDir())
	m := startMaster(t, master)
	set(t, master, "t", "k1", "v1")
	set(t, master, "t", "k2", "v2")
	snapshot(t, master) // prunes the WAL, so a fresh standby must resync from the snapshot
	set(t, master, "t", "k3", "v3")

	standbyDir := t.TempDir()
	standby := newNode(t, standbyDir)
	ctx, cancel := context.WithCancel(t.Context())
	applier := &cancellingApplier{Storage: standby.store, cancel: cancel, started: make(chan struct{})}
	sb := replication.NewStandby(m.Addr().String(), applier, standbyDir, 0, time.Millisecond, nil)
	sb.Start(ctx)
	select {
	case <-applier.started:
	case <-time.After(5 * time.Second):
		t.Fatal("resync never started")
	}
	sb.Stop()

	require.ErrorIs(t, sb.Terminal(), replication.ErrTerminal)
	requireFenced(t, standby)
	require.NoError(t, standby.writer.Close())

	reopened, lastLSN := reopenNode(t, standbyDir)
	require.LessOrEqual(t, lastLSN, uint64(2), "an interrupted resync must not leave a higher LSN than the state holds")
	sb2 := replication.NewStandby(m.Addr().String(), reopened.store, reopened.dir, lastLSN, time.Millisecond, nil)
	sb2.Start(t.Context())
	t.Cleanup(sb2.Stop)
	waitFor(t, "restarted standby to reseed", func() bool { return sb2.AppliedLSN() >= 3 })
	require.NoError(t, sb2.Terminal())
	require.Equal(t, "v3", mustGet(t, reopened, "t", "k3"))
}

// cancellingApplier cancels the standby's context the moment a resync starts applying, so the storage's own
// ResetToSnapshot fails part-way under cancellation.
type cancellingApplier struct {
	*storage.Storage
	cancel  context.CancelFunc
	started chan struct{}
}

func (c *cancellingApplier) ResetToSnapshot(ctx context.Context, dir string, lsn uint64, entries []engine.Entry) error {
	close(c.started)
	c.cancel()
	<-ctx.Done()
	return c.Storage.ResetToSnapshot(ctx, dir, lsn, entries)
}

// TestStandbyAheadOfMasterIsRefused pins both sides of the divergence check: the master refuses the handshake with a
// reseed instruction, and the standby goes terminal and fences its storage.
func TestStandbyAheadOfMasterIsRefused(t *testing.T) {
	t.Parallel()
	master := newNode(t, t.TempDir())
	m := startMaster(t, master)
	set(t, master, "t", "k", "v")

	standby := newNode(t, t.TempDir())
	for _, key := range []string{"a", "b", "c"} {
		set(t, standby, "own", key, "history") // LSN 3 on a history the master never wrote
	}
	sb := replication.NewStandby(m.Addr().String(), standby.store, standby.dir, standby.writer.LastLSN(),
		time.Millisecond, nil)
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)

	waitFor(t, "standby to be refused", func() bool { return sb.Terminal() != nil })
	err := sb.Terminal()
	require.ErrorIs(t, err, replication.ErrTerminal)
	require.ErrorContains(t, err, "standby LSN 3 is ahead of master LSN 1")
	require.ErrorContains(t, err, "reseed")
	requireFenced(t, standby)
	require.Equal(t, uint64(3), sb.AppliedLSN())
}

// TestStandbyDetectsMasterBehindItself covers the standby-side check on its own: a master whose heartbeat is behind
// the applied LSN has a shorter history than the standby applied.
func TestStandbyDetectsMasterBehindItself(t *testing.T) {
	t.Parallel()
	master := newFakeMaster(t)
	standby := newNode(t, t.TempDir())
	sb := replication.NewStandby(master.addr(), standby.store, standby.dir, 5, time.Millisecond, nil)
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)
	conn, lsn := master.accept()
	require.Equal(t, uint64(5), lsn)
	_, err := conn.Write(heartbeatFrame(3))
	require.NoError(t, err)
	waitFor(t, "standby to detect divergence", func() bool { return sb.Terminal() != nil })
	require.ErrorContains(t, sb.Terminal(), "master LSN 3 is behind this standby's applied LSN 5")
	requireFenced(t, standby)
	require.NoError(t, conn.Close())
}

// TestMasterRolledBackRefusesStandby restarts a master whose WAL lost its tail (a torn record). A standby that already
// applied the lost record is ahead and refused; one at the same LSN is refused too when the master's history is
// unverified, because the master cannot tell whether that standby holds the lost record; a fresh standby and one that
// resyncs from a snapshot are still served.
func TestMasterRolledBackRefusesStandby(t *testing.T) {
	t.Parallel()
	masterDir := t.TempDir()
	master := newNode(t, masterDir)
	m := startMaster(t, master)
	set(t, master, "t", "k1", "v1")
	set(t, master, "t", "k2", "v2")
	set(t, master, "t", "k3", "v3")

	caughtUpDir := t.TempDir()
	caughtUp := newNode(t, caughtUpDir)
	sb := replication.NewStandby(m.Addr().String(), caughtUp.store, caughtUp.dir, 0, time.Millisecond, nil)
	sb.Start(t.Context())
	waitFor(t, "standby to catch up", func() bool { return sb.AppliedLSN() == 3 })
	sb.Stop()
	require.NoError(t, m.Close())
	require.NoError(t, master.writer.Close())
	require.NoError(t, caughtUp.writer.Close())

	// Tear the last record so recovery truncates it: the master comes back at LSN 2.
	segments := walSegmentFiles(t, masterDir)
	require.Len(t, segments, 1)
	info, err := os.Stat(segments[0])
	require.NoError(t, err)
	require.NoError(t, os.Truncate(segments[0], info.Size()-3))
	recoveredMaster, masterLSN := reopenNode(t, masterDir)
	require.Equal(t, uint64(2), masterLSN)
	reason := "recovery truncated a torn WAL tail"
	m2, err := replication.NewMaster("127.0.0.1:0", recoveredMaster.writer, masterDir, nil,
		replication.WithUnverifiedHistory(reason))
	require.NoError(t, err)
	go m2.Serve(t.Context())
	t.Cleanup(func() { _ = m2.Close() })

	// The standby that applied LSN 3 is ahead of the rolled-back master.
	ahead, aheadLSN := reopenNode(t, caughtUpDir)
	require.Equal(t, uint64(3), aheadLSN)
	sbAhead := replication.NewStandby(m2.Addr().String(), ahead.store, ahead.dir, aheadLSN, time.Millisecond, nil)
	sbAhead.Start(t.Context())
	t.Cleanup(sbAhead.Stop)
	waitFor(t, "ahead standby to be refused", func() bool { return sbAhead.Terminal() != nil })
	require.ErrorContains(t, sbAhead.Terminal(), "ahead of master LSN 2")
	requireFenced(t, ahead)

	// A standby at LSN 2 asks to resume from the retained log; the master cannot vouch for its history.
	same := newNode(t, t.TempDir())
	sbSame := replication.NewStandby(m2.Addr().String(), same.store, same.dir, 2, time.Millisecond, nil)
	sbSame.Start(t.Context())
	t.Cleanup(sbSame.Stop)
	waitFor(t, "same-LSN standby to be refused", func() bool { return sbSame.Terminal() != nil })
	require.ErrorContains(t, sbSame.Terminal(), reason)
	require.ErrorContains(t, sbSame.Terminal(), "reseed")
	requireFenced(t, same)

	// A fresh standby starts from nothing and is served.
	fresh := newNode(t, t.TempDir())
	sbFresh := replication.NewStandby(m2.Addr().String(), fresh.store, fresh.dir, 0, time.Millisecond, nil)
	sbFresh.Start(t.Context())
	t.Cleanup(sbFresh.Stop)
	waitFor(t, "fresh standby to replicate", func() bool { return sbFresh.AppliedLSN() == 2 })
	require.NoError(t, sbFresh.Terminal())
	require.Equal(t, "v2", mustGet(t, fresh, "t", "k2"))
}

// TestUnverifiedMasterStillReseedsFromSnapshot: a standby far enough behind to need a snapshot resync is served by an
// unverified master, since the snapshot replaces its history wholesale.
func TestUnverifiedMasterStillReseedsFromSnapshot(t *testing.T) {
	t.Parallel()
	master := newNode(t, t.TempDir())
	set(t, master, "t", "k1", "v1")
	set(t, master, "t", "k2", "v2")
	snapshot(t, master) // prunes LSN 1-2 from the log
	set(t, master, "t", "k3", "v3")
	m, err := replication.NewMaster("127.0.0.1:0", master.writer, master.dir, nil,
		replication.WithUnverifiedHistory("torn tail"))
	require.NoError(t, err)
	go m.Serve(t.Context())
	t.Cleanup(func() { _ = m.Close() })

	standby := newNode(t, t.TempDir())
	sb := replication.NewStandby(m.Addr().String(), standby.store, standby.dir, 1, time.Millisecond, nil)
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)
	waitFor(t, "standby to resync from snapshot", func() bool { return sb.AppliedLSN() == 3 })
	require.NoError(t, sb.Terminal())
	require.Equal(t, "v3", mustGet(t, standby, "t", "k3"))
}

// TestReplicatedRecordAtOrBelowAppliedIsDivergence: a record the standby already applied can only mean the two logs
// disagree on what that LSN holds.
func TestReplicatedRecordAtOrBelowAppliedIsDivergence(t *testing.T) {
	t.Parallel()
	master := newFakeMaster(t)
	standby := newNode(t, t.TempDir())
	sb := replication.NewStandby(master.addr(), standby.store, standby.dir, 2, time.Millisecond, nil)
	sb.Start(t.Context())
	t.Cleanup(sb.Stop)
	conn, _ := master.accept()
	encoded, err := wal.EncodeRecord(wal.Record{LSN: 2, Command: wal.CommandSet, Args: []string{"t", "k", "v"}})
	require.NoError(t, err)
	_, err = conn.Write(append([]byte{'R'}, encoded...))
	require.NoError(t, err)
	waitFor(t, "standby to detect divergence", func() bool { return sb.Terminal() != nil })
	require.ErrorContains(t, sb.Terminal(), "master sent LSN 2 but this standby has already applied 2")
	requireFenced(t, standby)
	require.NoError(t, conn.Close())
}

// walSegmentFiles lists WAL segment paths in a directory.
func walSegmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		name := entry.Name()
		if len(name) > len(wal.WALPrefix) && name[:len(wal.WALPrefix)] == wal.WALPrefix {
			paths = append(paths, dir+string(os.PathSeparator)+name)
		}
	}
	return paths
}
