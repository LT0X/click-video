package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"douyin/package/upload"
	"douyin/rpc/user/user"
	"douyin/rpc/video/video"
)

func TestVideoUploadInitReturnsFastPassAndResumeParts(t *testing.T) {
	manager, store, uploadService, root, _ := newUploadServiceTestFixture(t)
	fastMD5 := md5Text("already stored")
	store.md5[fastMD5] = 77
	fastPass, err := uploadService.Init(context.Background(), 23, UploadInitRequest{
		FileName: "clip.mp4", FileSize: 10, FileMD5: fastMD5, TotalParts: 2, Title: "标题", Topic: "默认",
	})
	if err != nil || !fastPass.AlreadyUploaded || fastPass.VideoID != 77 {
		t.Fatalf("fast-pass Init() = (%+v, %v), want video 77", fastPass, err)
	}

	resumeID := "d5ab89a7-af58-455e-a241-a396a60ce2d3"
	meta := upload.Metadata{
		UploadID: resumeID, UserID: 23, FileName: "clip.mp4", FileSize: 10,
		FileMD5: md5Text("helloworld"), TotalParts: 2, Title: "标题", Topic: "默认",
		Status: upload.StatusUploading, CreatedAt: time.Now(),
	}
	if err := manager.WriteMetadata(meta); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.WritePart(resumeID, 1, 2, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPart(resumeID, 1); err != nil {
		t.Fatal(err)
	}
	resumed, err := uploadService.Init(context.Background(), 23, UploadInitRequest{
		UploadID: resumeID, FileName: meta.FileName, FileSize: meta.FileSize, FileMD5: meta.FileMD5,
		TotalParts: meta.TotalParts, Title: meta.Title, Topic: meta.Topic,
	})
	if err != nil || resumed.UploadID != resumeID || !reflect.DeepEqual(resumed.UploadedParts, []int{1}) {
		t.Fatalf("resume Init() = (%+v, %v), want upload %s part [1] under %s", resumed, err, resumeID, filepath.Clean(root))
	}
}

func TestVideoUploadMergeValidatesBeforePublishingAndIsIdempotent(t *testing.T) {
	manager, store, uploadService, _, calls := newUploadServiceTestFixture(t)
	uploadID := testUploadID
	fileContents := "helloworld"
	meta := upload.Metadata{
		UploadID: uploadID, UserID: 23, FileName: "clip.mp4", FileSize: int64(len(fileContents)),
		FileMD5: md5Text(fileContents), TotalParts: 2, Title: "标题", Topic: "默认", Status: upload.StatusUploading,
	}
	if err := manager.WriteMetadata(meta); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.WritePart(uploadID, 1, 2, strings.NewReader("hello"), 5); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPart(uploadID, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := uploadService.Merge(context.Background(), 23, uploadID, meta.FileMD5); err == nil {
		t.Fatal("Merge() error = nil with a missing part")
	}
	if calls.create != 0 {
		t.Fatalf("video create calls after missing part = %d, want 0", calls.create)
	}
	if _, err := manager.WritePart(uploadID, 2, 2, strings.NewReader("world"), 5); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkPart(uploadID, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := uploadService.Merge(context.Background(), 23, uploadID, md5Text("wrong")); err == nil {
		t.Fatal("Merge() error = nil with a mismatched MD5")
	}
	if calls.create != 0 {
		t.Fatalf("video create calls after bad MD5 = %d, want 0", calls.create)
	}

	merged, err := uploadService.Merge(context.Background(), 23, uploadID, meta.FileMD5)
	if err != nil || merged.VideoID != 88 {
		t.Fatalf("Merge() = (%+v, %v), want video ID 88", merged, err)
	}
	if _, err := uploadService.Merge(context.Background(), 23, uploadID, meta.FileMD5); err != nil {
		t.Fatalf("idempotent Merge() error = %v", err)
	}
	if calls.create != 1 || calls.workCount != 1 {
		t.Fatalf("RPC calls = create %d, work count %d; want one each", calls.create, calls.workCount)
	}
}

func TestVideoUploadChunkRejectsOutOfRangeAndNonUploadingState(t *testing.T) {
	manager, store, uploadService, _, _ := newUploadServiceTestFixture(t)
	meta := upload.Metadata{
		UploadID: testUploadID, UserID: 23, FileName: "clip.mp4", FileSize: 5,
		FileMD5: md5Text("hello"), TotalParts: 1, Title: "标题", Topic: "默认", Status: upload.StatusUploading,
	}
	if err := manager.WriteMetadata(meta); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(meta); err != nil {
		t.Fatal(err)
	}
	if _, err := uploadService.UploadChunk(context.Background(), 23, meta.UploadID, 2, 5, strings.NewReader("hello")); err == nil {
		t.Fatal("UploadChunk() error = nil for an out-of-range part number")
	}
	if err := store.SetStatus(meta.UploadID, upload.StatusComplete); err != nil {
		t.Fatal(err)
	}
	if _, err := uploadService.UploadChunk(context.Background(), 23, meta.UploadID, 1, 5, strings.NewReader("hello")); err == nil {
		t.Fatal("UploadChunk() error = nil for a completed session")
	}
}

const testUploadID = "9b2e6a61-1ab9-4d22-918d-8c2a2f7bd601"

type uploadServiceTestStore struct {
	sessions map[string]upload.Metadata
	parts    map[string]map[int]struct{}
	md5      map[string]uint64
}

func (s *uploadServiceTestStore) Create(meta upload.Metadata) error {
	meta.Status = upload.StatusUploading
	s.sessions[meta.UploadID] = meta
	s.parts[meta.UploadID] = make(map[int]struct{})
	return nil
}
func (s *uploadServiceTestStore) Get(id string) (upload.Metadata, error) {
	meta, ok := s.sessions[id]
	if !ok {
		return upload.Metadata{}, ErrUploadSessionNotFound
	}
	return meta, nil
}
func (s *uploadServiceTestStore) MarkPart(id string, part int) error {
	s.parts[id][part] = struct{}{}
	return nil
}
func (s *uploadServiceTestStore) UploadedParts(id string) ([]int, error) {
	parts := make([]int, 0, len(s.parts[id]))
	for part := range s.parts[id] {
		parts = append(parts, part)
	}
	return parts, nil
}
func (s *uploadServiceTestStore) SetStatus(id, status string) error {
	meta := s.sessions[id]
	meta.Status = status
	s.sessions[id] = meta
	return nil
}
func (s *uploadServiceTestStore) SetVideoID(id string, videoID uint64) error {
	meta := s.sessions[id]
	meta.VideoID = videoID
	s.sessions[id] = meta
	return nil
}
func (s *uploadServiceTestStore) SetVideoByMD5(fileMD5 string, videoID uint64) error {
	s.md5[fileMD5] = videoID
	return nil
}
func (s *uploadServiceTestStore) VideoByMD5(fileMD5 string) (uint64, bool, error) {
	videoID, ok := s.md5[fileMD5]
	return videoID, ok, nil
}
func (s *uploadServiceTestStore) RemoveParts(id string) error {
	delete(s.parts, id)
	return nil
}

type uploadRPCCallCounts struct {
	create    int
	workCount int
}

func newUploadServiceTestFixture(t *testing.T) (*upload.Manager, *uploadServiceTestStore, *VideoUploadService, string, *uploadRPCCallCounts) {
	t.Helper()
	root := t.TempDir()
	manager := upload.NewManager(upload.Config{
		TempDir: filepath.Join(root, "tmp"), VideoDir: filepath.Join(root, "videos"),
		PartSize: 5, MaxChunkSize: 10, MaxUploadSize: 50, MaxParts: 10, MergeConcurrency: 3,
	})
	store := &uploadServiceTestStore{sessions: make(map[string]upload.Metadata), parts: make(map[string]map[int]struct{}), md5: make(map[string]uint64)}
	cfg := UploadServiceConfig{
		Upload:   upload.Config{TempDir: filepath.Join(root, "tmp"), VideoDir: filepath.Join(root, "videos"), PartSize: 5, MaxChunkSize: 10, MaxUploadSize: 50, MaxParts: 10, MergeConcurrency: 3},
		CoverDir: filepath.Join(root, "covers"),
	}
	api := &VideoUploadService{config: cfg, manager: manager, state: store}
	calls := &uploadRPCCallCounts{}
	api.rpc = UploadRPC{
		CreateVideo: func(context.Context, *video.CreateVideoRequest) (*video.CreateVideoResponse, error) {
			calls.create++
			return &video.CreateVideoResponse{VideoID: 88}, nil
		},
		IncrementWorkCount: func(_ context.Context, req *user.IncrementWorkCountRequest) (*user.IncrementWorkCountResponse, error) {
			calls.workCount++
			if req.UserID == 0 || req.VideoID != 88 {
				return nil, errors.New("bad work-count request")
			}
			return &user.IncrementWorkCountResponse{Applied: true}, nil
		},
		UpdateVideoURL: func(context.Context, *video.UpdateVideoURLRequest) (*video.UpdateVideoURLResponse, error) {
			return &video.UpdateVideoURLResponse{Reply: 1}, nil
		},
	}
	return manager, store, api, root, calls
}

func md5Text(value string) string {
	sum := md5.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}
