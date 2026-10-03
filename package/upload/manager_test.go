package upload

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const testUploadID = "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601"

func newTestManager(t *testing.T) (*Manager, string, string) {
	t.Helper()
	root := t.TempDir()
	tempDir := filepath.Join(root, "tmp")
	videoDir := filepath.Join(root, "videos")
	return NewManager(Config{
		TempDir:          tempDir,
		VideoDir:         videoDir,
		PartSize:         5 * 1024 * 1024,
		MaxChunkSize:     10 * 1024 * 1024,
		MaxUploadSize:    50 * 1024 * 1024 * 1024,
		MaxParts:         10000,
		MergeConcurrency: 3,
	}), tempDir, videoDir
}

func TestWritePartStreamsAndUsesPartNumberName(t *testing.T) {
	manager, tempDir, _ := newTestManager(t)

	partMD5, err := manager.WritePart(testUploadID, 1, 2, strings.NewReader("hello"), 5)
	if err != nil {
		t.Fatalf("WritePart() error = %v", err)
	}
	if partMD5 != "5d41402abc4b2a76b9719d911017c592" {
		t.Fatalf("WritePart() md5 = %q", partMD5)
	}

	got, err := os.ReadFile(filepath.Join(tempDir, testUploadID, "part_00001"))
	if err != nil {
		t.Fatalf("read part: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("part contents = %q, want hello", got)
	}
}

func TestWritePartAllocatesLessThanTwoMiBForLargeReader(t *testing.T) {
	manager, _, _ := newTestManager(t)
	const size = int64(10 * 1024 * 1024)

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := manager.WritePart(testUploadID, 1, 1, &zeroReader{remaining: size}, size)
	if err != nil {
		t.Fatalf("WritePart() error = %v", err)
	}
	runtime.ReadMemStats(&after)
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated >= 2*1024*1024 {
		t.Fatalf("WritePart allocated %d bytes, want less than 2MiB", allocated)
	}
}

func TestMergeOrdersPartsAndVerifiesMD5(t *testing.T) {
	manager, _, _ := newTestManager(t)
	// 先写第二片，确认合并依据编号而不是到达顺序。
	if _, err := manager.WritePart(testUploadID, 2, 2, strings.NewReader("world"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.WritePart(testUploadID, 1, 2, strings.NewReader("hello "), 6); err != nil {
		t.Fatal(err)
	}

	path, err := manager.Merge(testUploadID, 2, 11, "5eb63bbbe01eeed093cb22bb8f5acdc3")
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read merged file: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("merged contents = %q, want %q", got, "hello world")
	}
}

func TestMergeRejectsMissingPartAndWrongMD5(t *testing.T) {
	t.Run("missing part", func(t *testing.T) {
		manager, _, videoDir := newTestManager(t)
		if _, err := manager.WritePart(testUploadID, 1, 2, strings.NewReader("hello "), 6); err != nil {
			t.Fatal(err)
		}
		_, err := manager.Merge(testUploadID, 2, 11, "5eb63bbbe01eeed093cb22bb8f5acdc3")
		if err == nil {
			t.Fatal("Merge() error = nil, want missing part error")
		}
		path := filepath.Join(videoDir, testUploadID+".mp4")
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("partial merged file exists at %q", path)
		}
	})

	t.Run("wrong MD5", func(t *testing.T) {
		manager, _, videoDir := newTestManager(t)
		if _, err := manager.WritePart(testUploadID, 1, 2, strings.NewReader("hello "), 6); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.WritePart(testUploadID, 2, 2, strings.NewReader("world"), 5); err != nil {
			t.Fatal(err)
		}
		_, err := manager.Merge(testUploadID, 2, 11, "00000000000000000000000000000000")
		if err == nil {
			t.Fatal("Merge() error = nil, want MD5 mismatch")
		}
		path := filepath.Join(videoDir, testUploadID+".mp4")
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("unverified merged file exists at %q", path)
		}
	})
}

func TestMergeLimitAllowsAtMostThree(t *testing.T) {
	manager, _, _ := newTestManager(t)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var wait sync.WaitGroup
	for i := 0; i < 4; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := manager.withMergeSlot(func() error {
				started <- struct{}{}
				<-release
				return nil
			}); err != nil {
				t.Errorf("withMergeSlot() error = %v", err)
			}
		}()
	}

	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("fewer than three merge operations entered")
		}
	}
	select {
	case <-started:
		t.Fatal("a fourth merge entered before a slot was released")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	wait.Wait()
}

type zeroReader struct{ remaining int64 }

func (r *zeroReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > r.remaining {
		n = int(r.remaining)
	}
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	r.remaining -= int64(n)
	return n, nil
}
