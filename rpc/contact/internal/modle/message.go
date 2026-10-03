package model

import (
	"douyin/rpc/contact/contact"
	"time"
)

type Message struct {
	ID         int64     `gorm:"primaryKey" json:"id"`
	EventID    *string   `gorm:"type:varchar(64);uniqueIndex:uk_message_event_id" json:"event_id,omitempty"`
	Content    string    `gorm:"not null;type:text" json:"content"`
	CreateTime time.Time `gorm:"not null;index" json:"create_time"` // 消息发送时间 yyyy-MM-dd HH:MM:ss
	FromUserID uint64    `gorm:"not null;index:idx_user_touser" json:"from_user_id"`
	ToUserID   uint64    `gorm:"not null;index:idx_user_touser" json:"to_user_id"`
}

func TransformRPCMessages(msg []Message) []*contact.Message {
	res := make([]*contact.Message, len(msg))
	for i, v := range msg {
		res[i] = &contact.Message{
			ID:         v.ID,
			Content:    v.Content,
			CreateTime: v.CreateTime.UnixMilli(),
			FromUserID: v.FromUserID,
			ToUserID:   int64(v.ToUserID),
		}
	}
	return res
}

func TransformRPCMessage(msg Message) *contact.Message {
	return &contact.Message{
		ID:         msg.ID,
		Content:    msg.Content,
		CreateTime: msg.CreateTime.UnixMilli(),
		FromUserID: msg.FromUserID,
		ToUserID:   int64(msg.ToUserID),
	}
}
