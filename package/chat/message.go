package chat

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

func NewMessage(fromUserID, toUserID uint64, content string, createdAt time.Time) (Message, error) {
	var randomID [16]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		return Message{}, fmt.Errorf("生成聊天消息事件 ID 失败: %w", err)
	}
	msg := Message{
		EventID: hex.EncodeToString(randomID[:]), Content: content, CreateTime: createdAt.UnixMilli(),
		FromUserID: fromUserID, ToUserID: toUserID,
	}
	if err := validateMessage(msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}
