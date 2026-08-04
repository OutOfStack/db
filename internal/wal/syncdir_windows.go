//go:build windows

package wal

// SyncDirectory is a no-op on Windows. Go's portable os API cannot open a Windows directory with the flags needed by
// FlushFileBuffers. NTFS journals rename/create metadata; file contents are still explicitly synced before publication.
func SyncDirectory(string) error { return nil }
