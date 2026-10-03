package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"douyin/package/upload"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

const (
	defaultUploadTTL    = 24 * time.Hour
	defaultUploadJitter = 5 * time.Minute
	defaultUploadMD5TTL = 7 * 24 * time.Hour
	defaultMaxParts     = 10000

	uploadMetaKeyPrefix  = "upload:"
	uploadPartsKeyPrefix = "upload_parts:"
	uploadMD5KeyPrefix   = "upload_md5:"
)

var ErrUploadSessionNotFound = errors.New("上传任务不存在或已过期")

// UploadState 在 video Redis 中保存断点续传状态和秒传映射。
type UploadState struct {
	client    *redis.Client
	manager   *upload.Manager
	ttlBase   time.Duration
	ttlJitter time.Duration
}

func NewUploadState(client *redis.Client, manager *upload.Manager, ttlBase, ttlJitter time.Duration) *UploadState {
	if ttlBase <= 0 {
		ttlBase = defaultUploadTTL
	}
	if ttlJitter < 0 {
		ttlJitter = 0
	}
	return &UploadState{client: client, manager: manager, ttlBase: ttlBase, ttlJitter: ttlJitter}
}

func (s *UploadState) Create(meta upload.Metadata) error {
	if s.client == nil {
		return errors.New("上传 Redis 客户端未初始化")
	}
	if !upload.ValidUploadID(meta.UploadID) {
		return errors.New("上传 ID 不合法")
	}
	meta.Status = upload.StatusUploading
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now()
	}
	ttl := randomizedUploadTTL(s.ttlBase, s.ttlJitter)
	pipe := s.client.Pipeline()
	pipe.HMSet(uploadMetaKey(meta.UploadID), uploadMetadataFields(meta))
	pipe.Expire(uploadMetaKey(meta.UploadID), ttl)
	pipe.Expire(uploadPartsKey(meta.UploadID), ttl)
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("保存上传任务状态失败: %w", err)
	}
	return nil
}

func (s *UploadState) Get(uploadID string) (upload.Metadata, error) {
	if !upload.ValidUploadID(uploadID) {
		return upload.Metadata{}, errors.New("上传 ID 不合法")
	}
	fields, err := s.client.HGetAll(uploadMetaKey(uploadID)).Result()
	if err != nil {
		return upload.Metadata{}, fmt.Errorf("读取上传任务状态失败: %w", err)
	}
	if len(fields) == 0 {
		return upload.Metadata{}, ErrUploadSessionNotFound
	}
	meta, err := parseUploadMetadata(uploadID, fields)
	if err != nil {
		return upload.Metadata{}, err
	}
	return meta, nil
}

func (s *UploadState) MarkPart(uploadID string, partNumber int) error {
	if !upload.ValidUploadID(uploadID) || partNumber < 1 {
		return errors.New("上传 ID 或分片序号不合法")
	}
	pipe := s.client.Pipeline()
	pipe.SAdd(uploadPartsKey(uploadID), partNumber)
	ttl := randomizedUploadTTL(s.ttlBase, s.ttlJitter)
	pipe.Expire(uploadMetaKey(uploadID), ttl)
	pipe.Expire(uploadPartsKey(uploadID), ttl)
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("登记已上传分片失败: %w", err)
	}
	return nil
}

func (s *UploadState) RemovePart(uploadID string, partNumber int) error {
	if err := s.client.SRem(uploadPartsKey(uploadID), partNumber).Err(); err != nil {
		return fmt.Errorf("清除过期分片记录失败: %w", err)
	}
	return nil
}

func (s *UploadState) UploadedParts(uploadID string) ([]int, error) {
	if !upload.ValidUploadID(uploadID) {
		return nil, errors.New("上传 ID 不合法")
	}
	values, err := s.client.SMembers(uploadPartsKey(uploadID)).Result()
	if err != nil {
		return nil, fmt.Errorf("读取已上传分片失败: %w", err)
	}
	parts := make([]int, 0, len(values))
	for _, value := range values {
		partNumber, err := strconv.Atoi(value)
		if err != nil || partNumber < 1 {
			continue
		}
		parts = append(parts, partNumber)
	}
	sort.Ints(parts)
	return parts, nil
}

func (s *UploadState) SetStatus(uploadID, status string) error {
	if !upload.ValidUploadID(uploadID) || status == "" {
		return errors.New("上传 ID 或任务状态不合法")
	}
	pipe := s.client.Pipeline()
	pipe.HSet(uploadMetaKey(uploadID), "status", status)
	ttl := randomizedUploadTTL(s.ttlBase, s.ttlJitter)
	pipe.Expire(uploadMetaKey(uploadID), ttl)
	pipe.Expire(uploadPartsKey(uploadID), ttl)
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("更新上传任务状态失败: %w", err)
	}
	return nil
}

func (s *UploadState) SetVideoID(uploadID string, videoID uint64) error {
	if !upload.ValidUploadID(uploadID) || videoID == 0 {
		return errors.New("上传 ID 或视频 ID 不合法")
	}
	pipe := s.client.Pipeline()
	pipe.HSet(uploadMetaKey(uploadID), "video_id", videoID)
	ttl := randomizedUploadTTL(s.ttlBase, s.ttlJitter)
	pipe.Expire(uploadMetaKey(uploadID), ttl)
	pipe.Expire(uploadPartsKey(uploadID), ttl)
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("保存上传任务视频 ID 失败: %w", err)
	}
	return nil
}

func (s *UploadState) RemoveParts(uploadID string) error {
	if !upload.ValidUploadID(uploadID) {
		return errors.New("上传 ID 不合法")
	}
	if err := s.client.Del(uploadPartsKey(uploadID)).Err(); err != nil {
		return fmt.Errorf("清理上传分片状态失败: %w", err)
	}
	return nil
}

func (s *UploadState) SetVideoByMD5(fileMD5 string, videoID uint64) error {
	if !upload.ValidMD5(fileMD5) || videoID == 0 {
		return errors.New("秒传映射参数不合法")
	}
	if err := s.client.Set(uploadMD5Key(fileMD5), videoID, randomizedUploadTTL(defaultUploadMD5TTL, s.ttlJitter)).Err(); err != nil {
		return fmt.Errorf("保存秒传映射失败: %w", err)
	}
	return nil
}

func (s *UploadState) VideoByMD5(fileMD5 string) (uint64, bool, error) {
	if !upload.ValidMD5(fileMD5) {
		return 0, false, errors.New("文件 MD5 格式不合法")
	}
	value, err := s.client.Get(uploadMD5Key(fileMD5)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("查询秒传映射失败: %w", err)
	}
	videoID, err := strconv.ParseUint(value, 10, 64)
	if err != nil || videoID == 0 {
		return 0, false, errors.New("秒传映射中的视频 ID 不合法")
	}
	return videoID, true, nil
}

func (s *UploadState) Delete(uploadID string) error {
	if !upload.ValidUploadID(uploadID) {
		return errors.New("上传 ID 不合法")
	}
	pipe := s.client.Pipeline()
	pipe.Del(uploadMetaKey(uploadID), uploadPartsKey(uploadID))
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("清理上传 Redis 状态失败: %w", err)
	}
	return nil
}

func (s *UploadState) Exists(uploadID string) (bool, error) {
	if !upload.ValidUploadID(uploadID) {
		return false, errors.New("上传 ID 不合法")
	}
	exists, err := s.client.Exists(uploadMetaKey(uploadID)).Result()
	if err != nil {
		return false, fmt.Errorf("检查上传任务状态失败: %w", err)
	}
	return exists > 0, nil
}

// RunUploadCleanup 每 30 分钟清理一次过期任务和 Redis 状态缺失的孤儿目录。
func RunUploadCleanup(ctx context.Context, manager *upload.Manager, state *UploadState, interval, ttl time.Duration) {
	if manager == nil || state == nil {
		zap.L().Error("启动视频上传清理任务失败：管理器或状态服务未初始化")
		return
	}
	if interval <= 0 {
		interval = 30 * time.Minute
	}
	if ttl <= 0 {
		ttl = defaultUploadTTL
	}
	cleanup := func() {
		ids, err := manager.UploadIDs()
		if err != nil {
			zap.L().Error("扫描视频上传目录失败", zap.Error(err))
			return
		}
		now := time.Now()
		for _, uploadID := range ids {
			canRemove, err := uploadDirectoryCanBeRemoved(now, manager, uploadID, ttl+state.ttlJitter, func(id string) error {
				_, err := state.Get(id)
				return err
			})
			if err != nil {
				zap.L().Warn("检查上传任务状态失败，暂不删除本地目录", zap.String("upload_id", uploadID), zap.Error(err))
				continue
			}
			if !canRemove {
				continue
			}
			if err := manager.RemoveUpload(uploadID); err != nil {
				zap.L().Error("清理孤儿视频分片失败", zap.String("upload_id", uploadID), zap.Error(err))
				continue
			}
			if err := state.Delete(uploadID); err != nil {
				zap.L().Warn("清理孤儿上传 Redis 状态失败", zap.String("upload_id", uploadID), zap.Error(err))
			}
		}
	}

	cleanup()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanup()
		}
	}
}

func uploadDirectoryCanBeRemoved(now time.Time, manager *upload.Manager, uploadID string, ttl time.Duration, readSession func(string) error) (bool, error) {
	if readSession == nil {
		return false, errors.New("上传状态查询函数未初始化")
	}
	err := readSession(uploadID)
	if err == nil {
		// Redis 仍保存会话（包括 merging 状态）时，本地分片仍属于活跃任务。
		return false, nil
	}
	if !errors.Is(err, ErrUploadSessionNotFound) {
		return false, err
	}
	return manager.UploadExpired(uploadID, now, ttl)
}

func validateUploadMetadata(meta upload.Metadata, config upload.Config) error {
	if meta.UserID == 0 {
		return errors.New("用户 ID 不合法")
	}
	if meta.FileName == "" || filepath.Base(meta.FileName) != meta.FileName || strings.ContainsAny(meta.FileName, `/\`) || !strings.EqualFold(filepath.Ext(meta.FileName), ".mp4") {
		return errors.New("文件名必须是 MP4 文件名")
	}
	if meta.FileSize <= 0 || config.MaxUploadSize <= 0 || meta.FileSize > config.MaxUploadSize {
		return errors.New("文件大小超出允许范围")
	}
	if !upload.ValidMD5(meta.FileMD5) {
		return errors.New("文件 MD5 格式不合法")
	}
	if config.PartSize <= 0 || config.MaxParts <= 0 {
		return errors.New("上传分片配置不合法")
	}
	expectedParts := int((meta.FileSize + config.PartSize - 1) / config.PartSize)
	if meta.TotalParts != expectedParts || meta.TotalParts < 1 || meta.TotalParts > config.MaxParts || meta.TotalParts > defaultMaxParts {
		return errors.New("分片数量与文件大小不匹配")
	}
	if strings.TrimSpace(meta.Title) == "" || utf8.RuneCountInString(meta.Title) > 30 {
		return errors.New("视频标题不能为空且不能超过 30 个字符")
	}
	if utf8.RuneCountInString(meta.Topic) > 63 {
		return errors.New("视频分类不能超过 63 个字符")
	}
	return nil
}

func availableUploadedParts(meta upload.Metadata, recorded []int, manager *upload.Manager) ([]int, error) {
	parts := make([]int, 0, len(recorded))
	sortedParts := append([]int(nil), recorded...)
	sort.Ints(sortedParts)
	lastPart := 0
	for _, partNumber := range sortedParts {
		if partNumber == lastPart || partNumber < 1 || partNumber > meta.TotalParts {
			continue
		}
		lastPart = partNumber
		exists, err := manager.HasPart(meta.UploadID, partNumber)
		if err != nil {
			return nil, err
		}
		if exists {
			parts = append(parts, partNumber)
		}
	}
	return parts, nil
}

func randomizedUploadTTL(base, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(int64(jitter)+1))
}

func uploadMetaKey(uploadID string) string  { return uploadMetaKeyPrefix + uploadID }
func uploadPartsKey(uploadID string) string { return uploadPartsKeyPrefix + uploadID }
func uploadMD5Key(fileMD5 string) string    { return uploadMD5KeyPrefix + fileMD5 }

func uploadMetadataFields(meta upload.Metadata) map[string]interface{} {
	return map[string]interface{}{
		"user_id":     meta.UserID,
		"file_name":   meta.FileName,
		"file_size":   meta.FileSize,
		"file_md5":    meta.FileMD5,
		"total_parts": meta.TotalParts,
		"title":       meta.Title,
		"topic":       meta.Topic,
		"status":      meta.Status,
		"video_id":    meta.VideoID,
		"created_at":  meta.CreatedAt.UnixNano(),
	}
}

func parseUploadMetadata(uploadID string, fields map[string]string) (upload.Metadata, error) {
	userID, err := strconv.ParseUint(fields["user_id"], 10, 64)
	if err != nil {
		return upload.Metadata{}, fmt.Errorf("上传任务 user_id 无效: %w", err)
	}
	fileSize, err := strconv.ParseInt(fields["file_size"], 10, 64)
	if err != nil {
		return upload.Metadata{}, fmt.Errorf("上传任务 file_size 无效: %w", err)
	}
	totalParts, err := strconv.Atoi(fields["total_parts"])
	if err != nil {
		return upload.Metadata{}, fmt.Errorf("上传任务 total_parts 无效: %w", err)
	}
	videoID, err := strconv.ParseUint(fields["video_id"], 10, 64)
	if err != nil {
		return upload.Metadata{}, fmt.Errorf("上传任务 video_id 无效: %w", err)
	}
	createdAt, err := strconv.ParseInt(fields["created_at"], 10, 64)
	if err != nil {
		return upload.Metadata{}, fmt.Errorf("上传任务 created_at 无效: %w", err)
	}
	return upload.Metadata{
		UploadID:   uploadID,
		UserID:     userID,
		FileName:   fields["file_name"],
		FileSize:   fileSize,
		FileMD5:    fields["file_md5"],
		TotalParts: totalParts,
		Title:      fields["title"],
		Topic:      fields["topic"],
		Status:     fields["status"],
		VideoID:    videoID,
		CreatedAt:  time.Unix(0, createdAt),
	}, nil
}
