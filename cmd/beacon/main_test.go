package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
