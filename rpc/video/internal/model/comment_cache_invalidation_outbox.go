package model

import "time"

// CommentCacheInvalidationOutbox 将评论事务提交与缓存失效事件可靠衔接。
type CommentCacheInvalidationOutbox struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	VideoID   uint64    `gorm:"not null;index"`
	CreatedAt time.Time `gorm:"not null;autoCreateTime"`
}

func (CommentCacheInvalidationOutbox) TableName() string {
	return "comment_cache_invalidation_outbox"
}
