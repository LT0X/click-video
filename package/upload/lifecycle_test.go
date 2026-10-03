package upload

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCleanupExpiredUploadDirs(t *testing.T) {
	root := t.TempDir()
	manager := NewManager(Config{TempDir: root, VideoDir: filepath.Join(root, "videos")})
	now := time.Now()
	oldID := "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601"
	recentID := "d5ab89a7-af58-455e-a241-a396a60ce2d3"
	for _, id := range []string{oldID, recentID} {
		if err := os.MkdirAll(filepath.Join(root, id), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	oldTime := now.Add(-25 * time.Hour)
	if err := os.Chtimes(filepath.Join(root, oldID), oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	recentTime := now.Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(root, recentID), recentTime, recentTime); err != nil {
		t.Fatal(err)
	}

	removed, err := manager.CleanupExpired(now, 24*time.Hour)
	if err != nil {
		t.Fatalf("CleanupExpired() error = %v", err)
	}
	if len(removed) != 1 || removed[0] != oldID {
		t.Fatalf("CleanupExpired() removed = %v, want [%s]", removed, oldID)
	}
	if _, err := os.Stat(filepath.Join(root, oldID)); !os.IsNotExist(err) {
		t.Fatalf("expired upload directory remains, stat error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, recentID)); err != nil {
		t.Fatalf("recent upload directory was removed: %v", err)
	}
}

func TestCleanupStaleMergeFiles(t *testing.T) {
	root := t.TempDir()
	videoDir := filepath.Join(root, "videos")
	scratchDir := filepath.Join(videoDir, ".upload-merge")
	if err := os.MkdirAll(scratchDir, 0o750); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	oldName := ".9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601-crashed.merge"
	recentName := ".d5ab89a7-af58-455e-a241-a396a60ce2d3-active.merge"
	unknownName := "unrecognized.merge"
	for _, name := range []string{oldName, recentName, unknownName} {
		if err := os.WriteFile(filepath.Join(scratchDir, name), []byte("scratch"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	oldTime := now.Add(-49 * time.Hour)
	if err := os.Chtimes(filepath.Join(scratchDir, oldName), oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	manager := NewManager(Config{VideoDir: videoDir})
	removed, err := manager.CleanupStaleMergeFiles(now, 48*time.Hour)
	if err != nil {
		t.Fatalf("CleanupStaleMergeFiles() error = %v", err)
	}
	if len(removed) != 1 || removed[0] != oldName {
		t.Fatalf("CleanupStaleMergeFiles() removed = %v, want [%s]", removed, oldName)
	}
	if _, err := os.Stat(filepath.Join(scratchDir, oldName)); !os.IsNotExist(err) {
		t.Fatalf("stale merge file remains, stat error = %v", err)
	}
	for _, name := range []string{recentName, unknownName} {
		if _, err := os.Stat(filepath.Join(scratchDir, name)); err != nil {
			t.Fatalf("non-stale merge file %q was removed: %v", name, err)
		}
	}
}

func TestWriteMetadataPersistsUploadSession(t *testing.T) {
	root := t.TempDir()
	manager := NewManager(Config{TempDir: root})
	meta := Metadata{
		UploadID: "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601",
		UserID:   42,
		FileName: "clip.mp4",
		FileSize: 123,
		FileMD5:  "5eb63bbbe01eeed093cb22bb8f5acdc3",
		Status:   StatusUploading,
	}
	if err := manager.WriteMetadata(meta); err != nil {
		t.Fatalf("WriteMetadata() error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(root, meta.UploadID, "meta.json"))
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}
	var got Metadata
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode meta.json: %v", err)
	}
	if got.UploadID != meta.UploadID || got.UserID != meta.UserID || got.FileName != meta.FileName || got.FileMD5 != meta.FileMD5 {
		t.Fatalf("persisted metadata = %+v, want %+v", got, meta)
	}
}
