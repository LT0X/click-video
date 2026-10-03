package llm

import (
	"context"
	"douyin/database"
	"douyin/model"
	"douyin/package/chat"
	"douyin/rpc/user/user"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	ChatGPTAvatar = "http://127.0.0.1:8000/static/avater/gpt.jpg"
	ChatGPTName   = "ChatGPT"
	ChatGPTID     = uint64(1)
)

type ChatMessageSender func(context.Context, chat.Message) error

var chatSender struct {
	sync.RWMutex
	send ChatMessageSender
}

func ConfigureChatMessageSender(sender ChatMessageSender) {
	chatSender.Lock()
	chatSender.send = sender
	chatSender.Unlock()
}

func currentChatMessageSender() ChatMessageSender {
	chatSender.RLock()
	sender := chatSender.send
	chatSender.RUnlock()
	return sender
}

func SendToChatGPT(userID uint64, content string) (chat.Message, error) {
	sender := currentChatMessageSender()
	msg, err := queueChatMessage(context.Background(), userID, ChatGPTID, content, sender)
	if err != nil {
		return chat.Message{}, err
	}
	go requestToChatGPT(userID, content)
	return msg, nil
}

func requestToChatGPT(userID uint64, content string) {
	answer := RequestToSparkAPI(content)
	if answer == "" {
		return
	}
	sender := currentChatMessageSender()
	if _, err := queueChatMessage(context.Background(), ChatGPTID, userID, answer, sender); err != nil {
		zap.L().Error("AI 回复消息写入聊天缓冲失败", zap.Error(err))
	}
}

func queueChatMessage(ctx context.Context, fromUserID, toUserID uint64, content string, sender ChatMessageSender) (chat.Message, error) {
	if sender == nil {
		return chat.Message{}, fmt.Errorf("AI 聊天消息缓冲服务未初始化")
	}
	msg, err := chat.NewMessage(fromUserID, toUserID, content, time.Now())
	if err != nil {
		return chat.Message{}, err
	}
	if err := sender(ctx, msg); err != nil {
		return chat.Message{}, err
	}
	return msg, nil
}

func RegisterChatGPT() {
	chatUser := &model.User{
		ID: ChatGPTID, Username: ChatGPTName, Avatar: ChatGPTAvatar,
	}
	_, err := database.RPC.UserRpc.CreateUser(context.Background(), &user.CreateUserRequest{
		User: model.TransformUserInfo(chatUser),
	})
	if err != nil {
		zap.L().Info("ChatGPT 用户已存在或注册 RPC 失败", zap.Error(err))
	}
}
