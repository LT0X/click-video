package upload

import (
	"time"

	"github.com/gofrs/uuid"
)

const (
	StatusUploading = "uploading"
	StatusMerging   = "merging"
	StatusComplete  = "complete"
	StatusFailed    = "failed"
)

// Metadata 保存一次上传在 Redis 中需要续传和发布的视频信息。
type Metadata struct {
	UploadID   string    `json:"upload_id"`
	UserID     uint64    `json:"user_id"`
	FileName   string    `json:"file_name"`
	FileSize   int64     `json:"file_size"`
	FileMD5    string    `json:"file_md5"`
	TotalParts int       `json:"total_parts"`
	Title      string    `json:"title"`
	Topic      string    `json:"topic"`
	Status     string    `json:"status"`
	VideoID    uint64    `json:"video_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// ValidUploadID 限定上传目录标识为 UUID，避免用户输入参与文件路径拼接。
func ValidUploadID(value string) bool {
	_, err := uuid.FromString(value)
	return err == nil
}

// ValidMD5 判断摘要是否为 32 位十六进制 MD5。
func ValidMD5(value string) bool {
	return isMD5(value)
}
