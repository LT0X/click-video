package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"douyin/package/cache"
	"douyin/package/upload"
	"douyin/rpc/user/user"
	"douyin/rpc/video/video"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
)

type UploadInitRequest struct {
	UploadID   string `json:"upload_id,omitempty"`
	FileName   string `json:"file_name"`
	FileSize   int64  `json:"file_size"`
	FileMD5    string `json:"file_md5"`
	TotalParts int    `json:"total_parts"`
	Title      string `json:"title"`
	Topic      string `json:"topic"`
}

type UploadInitResult struct {
	UploadID        string `json:"upload_id,omitempty"`
	UploadedParts   []int  `json:"uploaded_parts,omitempty"`
	AlreadyUploaded bool   `json:"already_uploaded"`
	VideoID         uint64 `json:"video_id,omitempty"`
}

type UploadChunkResult struct {
	PartNumber int    `json:"part_number"`
	MD5        string `json:"md5"`
}

type UploadMergeResult struct {
	VideoID uint64 `json:"video_id"`
	PlayURL string `json:"play_url"`
}

type UploadServiceConfig struct {
	Upload        upload.Config
	CoverDir      string
	PublicBaseURL string
}

type UploadSessionStore interface {
	Create(upload.Metadata) error
	Get(string) (upload.Metadata, error)
	MarkPart(string, int) error
	UploadedParts(string) ([]int, error)
	SetStatus(string, string) error
	SetVideoID(string, uint64) error
	SetVideoByMD5(uint64, string, uint64) error
	VideoByMD5(uint64, string) (uint64, bool, error)
	RemoveParts(string) error
}

type UploadRPC struct {
	CreateVideo        func(context.Context, *video.CreateVideoRequest) (*video.CreateVideoResponse, error)
	IncrementWorkCount func(context.Context, *user.IncrementWorkCountRequest) (*user.IncrementWorkCountResponse, error)
	UpdateVideoURL     func(context.Context, *video.UpdateVideoURLRequest) (*video.UpdateVideoURLResponse, error)
}

type UploadSnapshot func(videoPath, imagePath string, frameNumber int) (string, error)

type VideoUploadService struct {
	config   UploadServiceConfig
	manager  *upload.Manager
	state    UploadSessionStore
	rpc      UploadRPC
	snapshot UploadSnapshot
}

func NewVideoUploadService(config UploadServiceConfig, manager *upload.Manager, state UploadSessionStore, rpc UploadRPC, snapshot UploadSnapshot) *VideoUploadService {
	return &VideoUploadService{config: config, manager: manager, state: state, rpc: rpc, snapshot: snapshot}
}

func (s *VideoUploadService) Init(ctx context.Context, userID uint64, req UploadInitRequest) (UploadInitResult, error) {
	if userID == 0 {
		return UploadInitResult{}, errors.New("用户 ID 不合法")
	}
	meta := upload.Metadata{
		UploadID: req.UploadID, UserID: userID, FileName: req.FileName,
		FileSize: req.FileSize, FileMD5: strings.ToLower(req.FileMD5), TotalParts: req.TotalParts,
		Title: req.Title, Topic: req.Topic,
	}
	if err := validateUploadMetadata(meta, s.config.Upload); err != nil {
		return UploadInitResult{}, err
	}

	if req.UploadID != "" {
		existing, err := s.state.Get(req.UploadID)
		if err != nil {
			return UploadInitResult{}, err
		}
		if !sameUploadRequest(existing, meta) {
			return UploadInitResult{}, errors.New("上传任务信息与初始化请求不匹配")
		}
		if existing.Status == upload.StatusComplete {
			return UploadInitResult{UploadID: existing.UploadID, AlreadyUploaded: true, VideoID: existing.VideoID}, nil
		}
		if existing.Status != upload.StatusUploading && existing.Status != upload.StatusMerging {
			return UploadInitResult{}, errors.New("上传任务当前状态不允许续传")
		}
		parts, err := s.state.UploadedParts(existing.UploadID)
		if err != nil {
			return UploadInitResult{}, err
		}
		available, err := availableUploadedParts(existing, parts, s.manager)
		if err != nil {
			return UploadInitResult{}, err
		}
		return UploadInitResult{UploadID: existing.UploadID, UploadedParts: available}, nil
	}

	videoID, exists, err := s.state.VideoByMD5(userID, meta.FileMD5)
	if err != nil {
		return UploadInitResult{}, err
	}
	if exists {
		return UploadInitResult{AlreadyUploaded: true, VideoID: videoID}, nil
	}
	uploadID, err := uuid.NewV4()
	if err != nil {
		return UploadInitResult{}, fmt.Errorf("生成上传 ID 失败: %w", err)
	}
	meta.UploadID = uploadID.String()
	meta.Status = upload.StatusUploading
	meta.CreatedAt = time.Now()
	if err := s.manager.WriteMetadata(meta); err != nil {
		return UploadInitResult{}, err
	}
	if err := s.state.Create(meta); err != nil {
		if cleanupErr := s.manager.RemoveUpload(meta.UploadID); cleanupErr != nil {
			zap.L().Warn("清理初始化失败的上传目录时出错", zap.String("upload_id", meta.UploadID), zap.Error(cleanupErr))
		}
		return UploadInitResult{}, err
	}
	return UploadInitResult{UploadID: meta.UploadID, UploadedParts: []int{}}, nil
}

func (s *VideoUploadService) UploadChunk(ctx context.Context, userID uint64, uploadID string, partNumber int, declaredSize int64, src io.Reader) (UploadChunkResult, error) {
	if !upload.ValidUploadID(uploadID) || partNumber < 1 {
		return UploadChunkResult{}, errors.New("上传 ID 或分片序号不合法")
	}
	meta, err := s.state.Get(uploadID)
	if err != nil {
		return UploadChunkResult{}, err
	}
	if meta.UserID != userID {
		return UploadChunkResult{}, errors.New("无权访问该上传任务")
	}
	if meta.Status != upload.StatusUploading {
		return UploadChunkResult{}, errors.New("上传任务当前状态不接收分片")
	}
	if partNumber > meta.TotalParts {
		return UploadChunkResult{}, errors.New("分片序号超出总数")
	}
	expectedSize := expectedUploadPartSize(meta, partNumber, s.config.Upload.PartSize)
	if declaredSize != expectedSize {
		return UploadChunkResult{}, fmt.Errorf("第 %d 片大小应为 %d 字节", partNumber, expectedSize)
	}
	digest, err := s.manager.WritePart(uploadID, partNumber, meta.TotalParts, src, declaredSize)
	if err != nil {
		return UploadChunkResult{}, err
	}
	if err := s.state.MarkPart(uploadID, partNumber); err != nil {
		return UploadChunkResult{}, err
	}
	return UploadChunkResult{PartNumber: partNumber, MD5: digest}, nil
}

func (s *VideoUploadService) Merge(ctx context.Context, userID uint64, uploadID, expectedMD5 string) (UploadMergeResult, error) {
	if !upload.ValidUploadID(uploadID) || !upload.ValidMD5(expectedMD5) {
		return UploadMergeResult{}, errors.New("上传 ID 或文件 MD5 不合法")
	}
	meta, err := s.state.Get(uploadID)
	if err != nil {
		return UploadMergeResult{}, err
	}
	if meta.UserID != userID {
		return UploadMergeResult{}, errors.New("无权访问该上传任务")
	}
	if !strings.EqualFold(meta.FileMD5, expectedMD5) {
		return UploadMergeResult{}, errors.New("合并 MD5 与初始化信息不匹配")
	}
	if meta.Status == upload.StatusComplete {
		if meta.VideoID == 0 {
			return UploadMergeResult{}, errors.New("已完成上传缺少视频 ID")
		}
		return UploadMergeResult{VideoID: meta.VideoID, PlayURL: s.mediaURL(uploadPlayURL(uploadID))}, nil
	}
	if meta.Status != upload.StatusUploading && meta.Status != upload.StatusMerging {
		return UploadMergeResult{}, errors.New("上传任务当前状态不允许合并")
	}
	recorded, err := s.state.UploadedParts(uploadID)
	if err != nil {
		return UploadMergeResult{}, err
	}
	available, err := availableUploadedParts(meta, recorded, s.manager)
	if err != nil {
		return UploadMergeResult{}, err
	}
	if len(available) != meta.TotalParts {
		return UploadMergeResult{}, errors.New("上传分片不完整")
	}
	if meta.Status == upload.StatusUploading {
		if err := s.state.SetStatus(uploadID, upload.StatusMerging); err != nil {
			return UploadMergeResult{}, err
		}
	}
	videoPath, err := s.manager.Merge(uploadID, meta.TotalParts, meta.FileSize, meta.FileMD5)
	if err != nil {
		if resetErr := s.state.SetStatus(uploadID, upload.StatusUploading); resetErr != nil {
			zap.L().Warn("重置失败的合并任务状态时出错", zap.String("upload_id", uploadID), zap.Error(resetErr))
		}
		return UploadMergeResult{}, err
	}
	if s.rpc.CreateVideo == nil || s.rpc.IncrementWorkCount == nil {
		return UploadMergeResult{}, errors.New("视频发布 RPC 未初始化")
	}
	playURL := s.mediaURL(uploadPlayURL(uploadID))
	created, err := s.rpc.CreateVideo(ctx, &video.CreateVideoRequest{
		UploadID: uploadID,
		VideoID: &video.VideoInfo{
			AuthorID: userID, PlayURL: playURL, Title: meta.Title, Topic: meta.Topic,
			PublishTime: time.Now().UnixMilli(),
		},
	})
	if err != nil {
		return UploadMergeResult{}, fmt.Errorf("通过 video.rpc 创建上传视频失败: %w", err)
	}
	if created == nil || created.VideoID == 0 {
		return UploadMergeResult{}, errors.New("video.rpc 返回了无效的视频 ID")
	}
	if cache.VideoIDBloomFilter != nil {
		cache.VideoIDBloomFilter.AddString(strconv.FormatUint(created.VideoID, 10))
	}
	if cache.UserRedisClient != nil && cache.VideoRedisClient != nil {
		if err := cache.PublishVideo(userID, created.VideoID); err != nil {
			zap.L().Warn("清除发布列表缓存失败", zap.Uint64("video_id", created.VideoID), zap.Error(err))
		}
	}
	countResult, err := s.rpc.IncrementWorkCount(ctx, &user.IncrementWorkCountRequest{UserID: userID, VideoID: created.VideoID})
	if err != nil {
		return UploadMergeResult{}, fmt.Errorf("通过 user.rpc 更新作品数失败: %w", err)
	}
	if countResult == nil {
		return UploadMergeResult{}, errors.New("user.rpc 返回了空的作品数更新结果")
	}
	if err := s.state.SetVideoID(uploadID, created.VideoID); err != nil {
		return UploadMergeResult{}, err
	}
	if err := s.state.SetVideoByMD5(userID, meta.FileMD5, created.VideoID); err != nil {
		return UploadMergeResult{}, err
	}
	if err := s.state.SetStatus(uploadID, upload.StatusComplete); err != nil {
		return UploadMergeResult{}, err
	}
	if err := s.state.RemoveParts(uploadID); err != nil {
		zap.L().Warn("清理已完成上传的 Redis 分片记录失败", zap.String("upload_id", uploadID), zap.Error(err))
	}
	if err := s.manager.RemoveUpload(uploadID); err != nil {
		zap.L().Warn("清理已完成上传的本地分片失败", zap.String("upload_id", uploadID), zap.Error(err))
	}
	if s.snapshot != nil && s.rpc.UpdateVideoURL != nil {
		go s.extractCover(created.VideoID, meta.UploadID, videoPath, playURL)
	}
	return UploadMergeResult{VideoID: created.VideoID, PlayURL: playURL}, nil
}

func (s *VideoUploadService) extractCover(videoID uint64, uploadID, videoPath, playURL string) {
	imageName := uploadID + ".png"
	imagePath := filepath.Join(s.config.CoverDir, imageName)
	if _, err := s.snapshot(videoPath, imagePath, 1); err != nil {
		zap.L().Error("提取视频封面失败", zap.String("upload_id", uploadID), zap.Error(err))
		return
	}
	coverURL := s.mediaURL("/video/covers/" + imageName)
	if _, err := s.rpc.UpdateVideoURL(context.Background(), &video.UpdateVideoURLRequest{
		VideoID: videoID, PlayURL: playURL, CoverURl: coverURL,
	}); err != nil {
		zap.L().Error("通过 video.rpc 更新封面地址失败", zap.String("upload_id", uploadID), zap.Error(err))
	}
}

func sameUploadRequest(existing, requested upload.Metadata) bool {
	return existing.UserID == requested.UserID && existing.FileName == requested.FileName &&
		existing.FileSize == requested.FileSize && strings.EqualFold(existing.FileMD5, requested.FileMD5) &&
		existing.TotalParts == requested.TotalParts && existing.Title == requested.Title && existing.Topic == requested.Topic
}

func expectedUploadPartSize(meta upload.Metadata, partNumber int, partSize int64) int64 {
	if partNumber < 1 || partNumber > meta.TotalParts || partSize <= 0 {
		return 0
	}
	remaining := meta.FileSize - int64(partNumber-1)*partSize
	if remaining <= 0 {
		return 0
	}
	if remaining < partSize {
		return remaining
	}
	return partSize
}

func uploadPlayURL(uploadID string) string {
	return "/video/videos/" + uploadID + ".mp4"
}

func (s *VideoUploadService) mediaURL(path string) string {
	baseURL := strings.TrimRight(strings.TrimSpace(s.config.PublicBaseURL), "/")
	if baseURL == "" {
		return path
	}
	return baseURL + path
}
