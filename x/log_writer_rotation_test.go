package x

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogWriterBackupNameCollision(t *testing.T) {
	dir := t.TempDir()
	writer := &LogWriter{FilePath: filepath.Join(dir, "audit.log")}
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	first := filepath.Join(dir, "audit-2026-01-02T03-04-05.000.log")
	require.NoError(t, os.WriteFile(first, []byte("first"), 0600))
	compressed := filepath.Join(dir, "audit-2026-01-02T03-04-05.001.log.gz")
	require.NoError(t, os.WriteFile(compressed, []byte("compressed"), 0600))
	next, err := writer.nextBackupName(now)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "audit-2026-01-02T03-04-05.002.log"), next)
	// Even if no file remains (compression/retention), do not reuse its name.
	later, err := writer.nextBackupName(now.Add(-time.Hour))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "audit-2026-01-02T03-04-05.003.log"), later)
	data, err := os.ReadFile(first)
	require.NoError(t, err)
	require.Equal(t, "first", string(data))
	data, err = os.ReadFile(compressed)
	require.NoError(t, err)
	require.Equal(t, "compressed", string(data))
}

func TestLogWriterRotationPreservesExistingBackups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	writer, err := (&LogWriter{FilePath: path, MaxSize: 1, MaxAge: 1}).Init()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	// Force the logical clock ahead of wall time, so successive rotations
	// exercise collisions without relying on scheduler speed or sleeping.
	stamp := time.Now().UTC().Truncate(time.Millisecond).Add(time.Hour)
	writer.lastBackupTime = stamp
	backup := filepath.Join(filepath.Dir(path), "audit-"+stamp.Add(time.Millisecond).Format(backupTimeFormat)+".log")
	require.NoError(t, os.WriteFile(backup, []byte("older\n"), 0600))
	for _, record := range []string{"first\n", "second\n"} {
		_, err = writer.Write([]byte(record))
		require.NoError(t, err)
		writer.mu.Lock()
		err = writer.rotate()
		writer.mu.Unlock()
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	for i, want := range []string{"older\n", "first\n", "second\n"} {
		name := filepath.Join(filepath.Dir(path), "audit-"+stamp.Add(time.Duration(i+1)*time.Millisecond).Format(backupTimeFormat)+".log")
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, want, string(data))
	}
}
