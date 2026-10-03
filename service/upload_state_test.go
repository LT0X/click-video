package service

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"douyin/package/upload"
)

const stateTestUploadID = "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601"

func TestValidateUploadMetadataRejectsInvalidValues(t *testing.T) {
	config := upload.Config{
		PartSize:      5,
		MaxChunkSize:  10,
		MaxUploadSize: 5 * 10000,
		MaxParts:      10000,
	}
	valid := upload.Metadata{
		UserID:     7,
		FileName:   "clip.mp4",
		FileSize:   10,
		FileMD5:    "5eb63bbbe01eeed093cb22bb8f5acdc3",
		TotalParts: 2,
		Title:      "短视频",
		Topic:      "音乐",
	}
	tests := []struct {
		name string
		edit func(*upload.Metadata)
	}{
		{name: "invalid MD5", edit: func(meta *upload.Metadata) { meta.FileMD5 = "not-md5" }},
		{name: "negative size", edit: func(meta *upload.Metadata) { meta.FileSize = -1 }},
		{name: "part count mismatch", edit: func(meta *upload.Metadata) { meta.TotalParts = 3 }},
		{name: "too many parts", edit: func(meta *upload.Metadata) {
			meta.FileSize = int64(config.PartSize) * int64(config.MaxParts+1)
			meta.TotalParts = config.MaxParts + 1
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := valid
			tt.edit(&meta)
			if err := validateUploadMetadata(meta, config); err == nil {
				t.Fatal("validateUploadMetadata() error = nil, want invalid metadata error")
			}
		})
	}
}

func TestAvailableUploadedPartsIgnoresMissingFiles(t *testing.T) {
	root := t.TempDir()
	manager := upload.NewManager(upload.Config{
		TempDir:          filepath.Join(root, "tmp"),
		VideoDir:         filepath.Join(root, "videos"),
		PartSize:         5,
		MaxChunkSize:     10,
		MaxUploadSize:    50,
		MaxParts:         10,
		MergeConcurrency: 3,
	})
	if _, err := manager.WritePart(stateTestUploadID, 1, 2, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	meta := upload.Metadata{UploadID: stateTestUploadID, TotalParts: 2}

	// Redis Set 不保证 SMEMBERS 的返回顺序，也可能含有重复的旧记录。
	got, err := availableUploadedParts(meta, []int{2, 1, 2}, manager)
	if err != nil {
		t.Fatalf("availableUploadedParts() error = %v", err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("availableUploadedParts() = %v, want [1]", got)
	}
}

func TestRandomizedUploadTTLStaysWithinWindow(t *testing.T) {
	base := 24 * time.Hour
	jitter := 5 * time.Minute
	for i := 0; i < 100; i++ {
		got := randomizedUploadTTL(base, jitter)
		if got < base || got > base+jitter {
			t.Fatalf("randomizedUploadTTL() = %s, want range [%s, %s]", got, base, base+jitter)
		}
	}
}

func TestCleanupDecisionPreservesLiveOrUnreadableSessions(t *testing.T) {
	root := t.TempDir()
	manager := upload.NewManager(upload.Config{TempDir: root})
	oldID := stateTestUploadID
	recentID := "d5ab89a7-af58-455e-a241-a396a60ce2d3"
	for _, id := range []string{oldID, recentID} {
		if err := os.MkdirAll(filepath.Join(root, id), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	oldTime := time.Now().Add(-26 * time.Hour)
	if err := os.Chtimes(filepath.Join(root, oldID), oldTime, oldTime); err != nil {
		t.Fatal(err)
	}

	lookupLive := func(string) error { return nil }
	if got, err := uploadDirectoryCanBeRemoved(time.Now(), manager, oldID, 24*time.Hour, lookupLive); err != nil || got {
		t.Fatalf("active session cleanup = %v, %v; want false, nil", got, err)
	}
	lookupUnavailable := func(string) error { return errors.New("redis unavailable") }
	if got, err := uploadDirectoryCanBeRemoved(time.Now(), manager, oldID, 24*time.Hour, lookupUnavailable); err == nil || got {
		t.Fatalf("unreadable session cleanup = %v, %v; want false, error", got, err)
	}
	lookupMissing := func(string) error { return ErrUploadSessionNotFound }
	if got, err := uploadDirectoryCanBeRemoved(time.Now(), manager, recentID, 24*time.Hour, lookupMissing); err != nil || got {
		t.Fatalf("recent orphan cleanup = %v, %v; want false, nil", got, err)
	}
	if got, err := uploadDirectoryCanBeRemoved(time.Now(), manager, oldID, 24*time.Hour, lookupMissing); err != nil || !got {
		t.Fatalf("expired orphan cleanup = %v, %v; want true, nil", got, err)
	}
}
