package model

// WorkCountVideo 记录哪些视频已经计入作者作品数，按视频 ID 保证重试幂等。
type WorkCountVideo struct {
	VideoID uint64 `gorm:"primaryKey;autoIncrement:false" json:"video_id"`
	UserID  uint64 `gorm:"not null;index" json:"user_id"`
}

func (WorkCountVideo) TableName() string { return "work_count_video" }
