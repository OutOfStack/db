//go:build !windows

package wal_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/OutOfStack/db/internal/wal"
	"github.com/stretchr/testify/require"
)

// SIGKILL preserves the OS page cache, so recovery may include unsynced records. The sync watermark is a lower bound,
// not an exact loss target; every recovered record must still belong to the issued, contiguous sequence.
func TestAbruptDeathRecoveryBySyncWatermark(t *testing.T) {
	t.Parallel()
	for _, policy := range []wal.SyncPolicy{wal.SyncAlways, wal.SyncEverySec, wal.SyncNo} {
		t.Run(string(policy), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWALCrashHelper$")
			cmd.Env = append(os.Environ(), "DB_TEST_CRASH_DIR="+dir, "DB_TEST_CRASH_POLICY="+string(policy))
			output, err := cmd.StdoutPipe()
			require.NoError(t, err)
			input, err := cmd.StdinPipe()
			require.NoError(t, err)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			require.NoError(t, cmd.Start())
			waited := false
			defer func() {
				if !waited {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			scanner := bufio.NewScanner(output)
			require.True(t, scanner.Scan(), "helper did not report its initial watermark")
			var acknowledged, synced uint64
			_, err = fmt.Sscan(scanner.Text(), &acknowledged, &synced)
			require.NoError(t, err)
			require.Equal(t, uint64(32), acknowledged)
			if policy != wal.SyncNo {
				require.Equal(t, acknowledged, synced)
			}
			_, err = fmt.Fprintln(input, "continue")
			require.NoError(t, err)
			for range 8 {
				require.True(t, scanner.Scan(), "helper stopped before acknowledging writes")
				_, err = fmt.Sscan(scanner.Text(), &acknowledged, &synced)
				require.NoError(t, err)
			}
			require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
			err = cmd.Wait()
			waited = true
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, stderr.String())
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			require.True(t, ok)
			require.Equal(t, syscall.SIGKILL, status.Signal())
			lock, err := datadir.Acquire(dir)
			require.NoError(t, err)
			defer func() { require.NoError(t, lock.Close()) }()
			next := uint64(1)
			recovered, err := wal.NewReader(dir, nil).Replay(0, func(record wal.Record) error {
				require.Equal(t, next, record.LSN)
				require.Equal(t, wal.CommandSet, record.Command)
				require.Equal(t, []string{"t", strconv.FormatUint(next, 10), "value-" + strconv.FormatUint(next, 10)}, record.Args)
				next++
				return nil
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, recovered, synced)
			require.LessOrEqual(t, recovered, uint64(96))
			if policy == wal.SyncAlways {
				require.GreaterOrEqual(t, recovered, acknowledged)
			}
			t.Logf("acknowledged=%d synced=%d recovered=%d", acknowledged, synced, recovered)
		})
	}
}

func TestWALCrashHelper(t *testing.T) {
	dir := os.Getenv("DB_TEST_CRASH_DIR")
	if dir == "" {
		return
	}
	lock, err := datadir.Acquire(dir)
	require.NoError(t, err)
	defer func() { _ = lock.Close() }()
	policy := wal.SyncPolicy(os.Getenv("DB_TEST_CRASH_POLICY"))
	writer, err := wal.OpenWriter(wal.WriterConfig{Dir: dir, Sync: policy, SegmentSize: 1024}, 0)
	require.NoError(t, err)
	// Deliberately leave the writer open: the parent terminates this process without running cleanup.
	appendRecord := func(i int) {
		_, appendErr := writer.Append(t.Context(), wal.CommandSet, []string{"t", strconv.Itoa(i), "value-" + strconv.Itoa(i)})
		require.NoError(t, appendErr)
	}
	for i := 1; i <= 32; i++ {
		appendRecord(i)
	}
	if policy == wal.SyncEverySec {
		require.Eventually(t, func() bool { return writer.Status().SyncedLSN >= 32 }, 5*time.Second, 10*time.Millisecond)
	}
	report := func() {
		status := writer.Status()
		_, writeErr := fmt.Fprintf(os.Stdout, "%d %d\n", status.LastLSN, status.SyncedLSN)
		require.NoError(t, writeErr)
	}
	report()
	scanner := bufio.NewScanner(os.Stdin)
	require.True(t, scanner.Scan())
	for i := 33; i <= 96; i++ {
		appendRecord(i)
		report()
	}
	// Keep the child alive even if it outpaces the parent reading acknowledgments.
	scanner.Scan()
}
