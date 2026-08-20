package datadir_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/OutOfStack/db/internal/datadir"
	"github.com/stretchr/testify/require"
)

func TestLockExcludesOtherOwnersAndReleases(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	lock, err := datadir.Acquire(dir)
	require.NoError(t, err)
	_, err = datadir.Acquire(dir)
	require.Error(t, err)
	require.NoError(t, lock.Close())

	lock, err = datadir.Acquire(dir)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
	_, err = os.Stat(filepath.Join(dir, ".db.lock"))
	require.NoError(t, err)
}

func TestLockReleasesWhenOwnerProcessDies(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=TestLockHelperProcess")
	cmd.Env = append(os.Environ(), "DB_LOCK_HELPER="+dir)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	scanner := bufio.NewScanner(stdout)
	require.True(t, scanner.Scan(), "helper exited before acquiring lock")
	require.Equal(t, "locked", scanner.Text())
	_, err = datadir.Acquire(dir)
	require.Error(t, err)

	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())
	lock, err := datadir.Acquire(dir)
	require.NoError(t, err)
	require.NoError(t, lock.Close())
}

func TestLockHelperProcess(t *testing.T) {
	dir := os.Getenv("DB_LOCK_HELPER")
	if dir == "" {
		return
	}
	lock, err := datadir.Acquire(dir)
	require.NoError(t, err)
	defer func() { _ = lock.Close() }()
	fmt.Println("locked")
	select {}
}
