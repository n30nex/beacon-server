package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForPostgresRetriesUntilReady(t *testing.T) {
	var attempts atomic.Int32
	err := waitForPostgres(context.Background(), func(context.Context) error {
		if attempts.Add(1) < 3 {
			return errors.New("connection refused")
		}
		return nil
	}, time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("waitForPostgres() error = %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}
}

func TestWaitForPostgresTimesOutWithLastError(t *testing.T) {
	err := waitForPostgres(context.Background(), func(context.Context) error {
		return errors.New("connection refused")
	}, 10*time.Millisecond, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("waitForPostgres() error = %v, want last probe error", err)
	}
}

func TestLocalBackupSnapshotRejectsInvalidCreatedAt(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("BEACON_BACKUP_DIR", dir)
	manifest := `{"createdAt":"not-rfc3339","listVerified":true,"scratchRestoreVerified":true}`
	if err := os.WriteFile(filepath.Join(dir, "latest.manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot := localBackupSnapshot()
	if snapshot.Status != "invalid" {
		t.Fatalf("status = %q, want invalid", snapshot.Status)
	}
}

func TestLocalBackupSnapshotReadsSanitizedMetadata(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("BEACON_BACKUP_DIR", dir)
	createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	manifest := `{"createdAt":"` + createdAt.Format(time.RFC3339) + `","listVerified":true,"scratchRestoreVerified":true}`
	if err := os.WriteFile(filepath.Join(dir, "latest.manifest.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot := localBackupSnapshot()
	if snapshot.Status != "ok" || !snapshot.ListVerified || !snapshot.RestoreVerified {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.LastBackupAt != createdAt.UnixMilli() {
		t.Fatalf("lastBackupAt = %d, want %d", snapshot.LastBackupAt, createdAt.UnixMilli())
	}
}
