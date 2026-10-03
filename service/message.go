package service

import (
	"context"
	"douyin/database"
	"douyin/model"
	"douyin/package/cache"
	"douyin/package/chat"
	"douyin/package/constant"
	"douyin/package/llm"
	"douyin/package/metrics"
	"douyin/response"
	"douyin/rpc/contact/contact"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type MessageService struct {
	// 1-发送消息
	ActionType string `query:"action_type"`
	// 消息内容
	Content string `query:"content"`
	// 对方用户id
	ToUserID uint64 `query:"to_user_id"`
	// 用户鉴权token
	Token string `query:"token"`
	// //上次最新消息的时间（新增字段-apk更新中）
	Pre_msg_time      int64 `query:"pre_msg_time"`
	lastQueuedMessage chat.Message
}

func (service *MessageService) LastQueuedMessage() chat.Message { return service.lastQueuedMessage }

func (service *MessageService) MessageAction(loginUserID uint64) error {
	// TODO 可能还得限制一下消息长度
	if loginUserID == service.ToUserID {
		err := fmt.Errorf("不能给自己发送消息")
		return err
	} else if service.Content == "" {
		err := fmt.Errorf("消息内容为空 发送失败")
		return err
	} else if service.ActionType != "1" {
		err := fmt.Errorf("ActionType 错误")
		return err
	}
	// 给ChatGPT发送消息
	if service.ToUserID == llm.ChatGPTID {
		msg, err := llm.SendToChatGPT(loginUserID, service.Content)
		if err == nil {
			service.lastQueuedMessage = msg
		}
		return err
	}
	// 发送的id是不是朋友
	isfollowing, err := cache.IsFollow(loginUserID, service.ToUserID)
	if err != nil {
		zap.L().Warn(constant.CacheMiss)
		followed, rpcErr := database.RPC.ContactRpc.IsFollowed(context.TODO(), &contact.IsFollowedRequest{
			UserID: int64(loginUserID), ID: int64(service.ToUserID),
		})
		if rpcErr != nil {
			err = rpcErr
		} else {
			isfollowing, err = followed.Bool, nil
		}
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}
		go func() {
			followingResp, err := database.RPC.ContactRpc.SelectFollowingByUserID(context.Background(), &contact.SelectFollowingByUserIDRequest{UserID: loginUserID})
			if err != nil {
				zap.L().Error(err.Error())
				return
			}
			err = cache.SetFollowUserIDSet(loginUserID, followingResp.UserID)
			if err != nil {
				zap.L().Error(err.Error())
			}
		}()
	}
	if !isfollowing {
		err := fmt.Errorf("对方不是你的好友")
		return err
	}
	isfollowied, err := cache.IsFollow(service.ToUserID, loginUserID)
	if err != nil {
		zap.L().Warn(constant.CacheMiss)
		followed, rpcErr := database.RPC.ContactRpc.IsFollowed(context.TODO(), &contact.IsFollowedRequest{
			UserID: int64(service.ToUserID), ID: int64(loginUserID),
		})
		if rpcErr != nil {
			err = rpcErr
		} else {
			isfollowied, err = followed.Bool, nil
		}
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}
		go func() {
			followingResp, err := database.RPC.ContactRpc.SelectFollowingByUserID(context.Background(), &contact.SelectFollowingByUserIDRequest{UserID: service.ToUserID})
			if err != nil {
				zap.L().Error(err.Error())
				return
			}
			err = cache.SetFollowUserIDSet(service.ToUserID, followingResp.UserID)
			if err != nil {
				zap.L().Error(err.Error())
			}
		}()
	}
	if !isfollowied {
		err := fmt.Errorf("对方不是你的好友")
		return err
	}
	dispatcher, chatCache := currentChatPipeline()
	if dispatcher == nil {
		return fmt.Errorf("聊天消息缓冲服务未初始化")
	}
	msg, err := chat.NewMessage(loginUserID, service.ToUserID, service.Content, time.Now())
	if err != nil {
		return err
	}
	if err := dispatcher.Send(context.Background(), msg); err != nil {
		return err
	}
	service.lastQueuedMessage = msg
	if chatCache != nil {
		go func() {
			if err := chatCache.OnMessageQueued(context.Background(), msg); err != nil {
				zap.L().Warn("更新聊天消息缓存失败", zap.Error(err))
			}
		}()
	}
	return nil
}

func (service *MessageService) MessageChat(loginUserID uint64) (messageResponse *response.MessageResponse, requestErr error) {
	startedAt := time.Now()
	source := "other"
	cacheResult := ""
	defer func() {
		recordChatHistoryMetrics(metrics.Default, source, cacheResult, requestErr, time.Since(startedAt))
	}()

	if loginUserID == service.ToUserID {
		err := fmt.Errorf("ToUserID不能是自己")
		return nil, err
	}
	source = "database"
	_, chatCache := currentChatPipeline()
	if chatCache != nil {
		cached, hit, err := chatCache.GetHistory(context.Background(), loginUserID, service.ToUserID, service.Pre_msg_time)
		if err != nil {
			cacheResult = "error"
			zap.L().Warn("读取聊天历史缓存失败，回源 contact.rpc", zap.Error(err))
		} else if hit {
			cacheResult = "hit"
			source = "cache"
			return messageResponseFromCache(cached), nil
		} else {
			cacheResult = "miss"
		}
	}
	resp, err := database.RPC.ContactRpc.MessageList(context.TODO(), &contact.MessageListRequest{
		UserID:   loginUserID,
		ToUserID: service.ToUserID,
		MsgTime:  service.Pre_msg_time,
	})
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	msgs := model.TransformMessages(resp.Massages)
	cacheMessages := make([]chat.Message, 0, len(msgs))
	for _, msg := range msgs {
		cacheMessages = append(cacheMessages, chat.Message{
			Content: msg.Content, CreateTime: msg.CreateTime.UnixMilli(),
			FromUserID: msg.FromUserID, ID: msg.ID, ToUserID: msg.ToUserID,
		})
	}
	if chatCache != nil && service.Pre_msg_time <= 0 {
		go func() {
			if err := chatCache.FillHistory(context.Background(), loginUserID, service.ToUserID, cacheMessages); err != nil {
				zap.L().Warn("异步回填聊天历史缓存失败", zap.Error(err))
			}
		}()
	}
	return messageResponseFromCache(cacheMessages), nil
}

func recordChatHistoryMetrics(registry *metrics.Registry, source, cacheResult string, requestErr error, duration time.Duration) {
	if cacheResult != "" {
		registry.ObserveChatHistoryCacheAccess(cacheResult)
	}
	result := "success"
	if requestErr != nil {
		result = "error"
	}
	registry.ObserveChatHistoryRequest(source, result, duration)
}

func messageResponseFromCache(messages []chat.Message) *response.MessageResponse {
	msgsList := make([]response.Message, 0, len(messages))
	for _, msg := range messages {
		msgsList = append(msgsList, response.Message{
			Content: msg.Content, CreateTime: msg.CreateTime, FromUserID: msg.FromUserID,
			ID: msg.ID, ToUserID: msg.ToUserID,
		})
	}
	return &response.MessageResponse{
		MessageList: msgsList,
		StatusCode:  response.Success,
		StatusMsg:   "消息列表加载成功",
	}
}
