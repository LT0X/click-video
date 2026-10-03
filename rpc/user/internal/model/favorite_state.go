package model

import "time"

// FavoriteActionState 保留每个用户/视频对已接受的最新动作序号。
type FavoriteActionState struct {
	UserID             uint64 `gorm:"primaryKey;autoIncrement:false;index:idx_favorite_action_state_video_id,priority:2"`
	VideoID            uint64 `gorm:"primaryKey;autoIncrement:false;index:idx_favorite_action_state_video_id,priority:1"`
	LastIssuedSequence uint64 `gorm:"not null;default:0"`
	LastActionSequence uint64 `gorm:"not null;default:0"`
}

func (FavoriteActionState) TableName() string { return "favorite_action_state" }

// FavoriteVideoCountState 在 user.rpc 内串行生成视频点赞计数版本。
type FavoriteVideoCountState struct {
	VideoID       uint64 `gorm:"primaryKey;autoIncrement:false"`
	FavoriteCount int64  `gorm:"not null;default:0"`
	CountVersion  uint64 `gorm:"not null;default:0"`
}

func (FavoriteVideoCountState) TableName() string { return "favorite_video_count_state" }

// FavoriteCountOutbox 与 favorite/user 更新同事务，保证计数事件可重试发布。
type FavoriteCountOutbox struct {
	ID            uint64     `gorm:"primaryKey;autoIncrement"`
	EventID       uint64     `gorm:"not null;uniqueIndex:idx_favorite_count_outbox_event"`
	UserID        uint64     `gorm:"not null"`
	AuthorID      uint64     `gorm:"not null;default:0"`
	VideoID       uint64     `gorm:"not null;uniqueIndex:idx_favorite_count_outbox_version,priority:1"`
	CountVersion  uint64     `gorm:"not null;uniqueIndex:idx_favorite_count_outbox_version,priority:2"`
	Delta         int64      `gorm:"not null"`
	FavoriteCount int64      `gorm:"not null"`
	CreatedAt     time.Time  `gorm:"not null;autoCreateTime"`
	PublishedAt   *time.Time `gorm:"index:idx_favorite_count_outbox_pending"`
}

func (FavoriteCountOutbox) TableName() string { return "favorite_count_outbox" }
