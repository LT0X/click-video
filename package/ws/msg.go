package ws

import (
	"context"
	"douyin/package/chat"
	"douyin/package/constant"
	"douyin/service"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/contrib/websocket"
	"go.uber.org/zap"
)

type clientMessage struct {
	Type     string `json:"type"`
	ToUserID uint64 `json:"to_user_id"`
	Content  string `json:"content"`
}

var runtime struct {
	sync.RWMutex
	registry *chat.Registry
	routes   *chat.RedisRoutes
	dispatch *chat.Dispatcher
	node     string
}

func ConfigureChat(registry *chat.Registry, routes *chat.RedisRoutes, dispatcher *chat.Dispatcher, nodeAddress string) {
	runtime.Lock()
	runtime.registry, runtime.routes, runtime.dispatch, runtime.node = registry, routes, dispatcher, nodeAddress
	runtime.Unlock()
}

func currentRuntime() (*chat.Registry, *chat.RedisRoutes, *chat.Dispatcher, string) {
	runtime.RLock()
	registry, routes, dispatch, node := runtime.registry, runtime.routes, runtime.dispatch, runtime.node
	runtime.RUnlock()
	return registry, routes, dispatch, node
}

func HandleWebSocket() func(*websocket.Conn) {
	return func(c *websocket.Conn) {
		userID, ok := c.Locals(constant.UserID).(uint64)
		if !ok || userID == 0 {
			_ = c.Close()
			return
		}
		var defaultToUserID uint64
		if rawToUserID := c.Query("to_user_id"); rawToUserID != "" {
			parsed, err := strconv.ParseUint(rawToUserID, 10, 64)
			if err != nil {
				zap.L().Warn("WebSocket 对端用户 ID 无效", zap.Error(err))
				_ = c.Close()
				return
			}
			defaultToUserID = parsed
		}
		registry, routes, dispatcher, nodeAddress := currentRuntime()
		if registry == nil || routes == nil || dispatcher == nil || nodeAddress == "" {
			zap.L().Error("聊天 WebSocket 尚未初始化")
			_ = c.Close()
			return
		}

		client := chat.NewClient(c)
		registry.Register(userID, client)
		lease, err := routes.Register(context.Background(), userID, nodeAddress)
		if err != nil {
			registry.Remove(userID, client)
			zap.L().Error("注册 WebSocket 在线路由失败", zap.Uint64("user_id", userID), zap.Error(err))
			_ = client.Close()
			return
		}
		defer func() {
			registry.Remove(userID, client)
			if err := routes.Remove(context.Background(), userID, lease); err != nil {
				zap.L().Warn("清理 WebSocket 在线路由失败", zap.Uint64("user_id", userID), zap.Error(err))
			}
		}()

		if err := c.SetReadDeadline(time.Now().Add(chat.RouteTTL)); err != nil {
			zap.L().Warn("设置 WebSocket 心跳超时失败", zap.Error(err))
		}
		for {
			_, payload, err := c.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					zap.L().Info("WebSocket 连接结束", zap.Uint64("user_id", userID), zap.Error(err))
				}
				return
			}
			incoming, err := parseClientMessage(payload, defaultToUserID)
			if err != nil {
				if writeErr := client.WriteJSON(map[string]interface{}{"type": "error", "message": err.Error()}); writeErr != nil {
					zap.L().Warn("WebSocket 错误响应发送失败", zap.Error(writeErr))
					return
				}
				continue
			}
			switch incoming.Type {
			case "ping":
				refreshed, err := routes.Refresh(context.Background(), userID, lease)
				if err != nil || !refreshed {
					zap.L().Warn("WebSocket 心跳续期失败", zap.Uint64("user_id", userID), zap.Error(err))
					return
				}
				if err := c.SetReadDeadline(time.Now().Add(chat.RouteTTL)); err != nil {
					zap.L().Warn("续期 WebSocket 读超时失败", zap.Error(err))
					return
				}
				if err := client.WriteJSON(map[string]string{"type": "pong"}); err != nil {
					return
				}
			case "get":
				messageService := service.MessageService{ToUserID: incoming.ToUserID}
				result, err := messageService.MessageChat(userID)
				if err != nil {
					zap.L().Error("WebSocket 加载聊天历史失败", zap.Error(err))
					_ = client.WriteJSON(map[string]string{"type": "error", "message": err.Error()})
					continue
				}
				if err := client.WriteJSON(result); err != nil {
					return
				}
			case "message":
				messageService := &service.MessageService{ActionType: "1", ToUserID: incoming.ToUserID, Content: incoming.Content}
				if err := messageService.MessageAction(userID); err != nil {
					_ = client.WriteJSON(map[string]string{"type": "error", "message": err.Error()})
					continue
				}
				queued := messageService.LastQueuedMessage()
				if err := client.WriteJSON(map[string]interface{}{
					"type": "accepted", "event_id": queued.EventID, "id": queued.ID,
					"from_user_id": queued.FromUserID, "to_user_id": queued.ToUserID,
					"create_time": queued.CreateTime, "content": queued.Content,
				}); err != nil {
					return
				}
			default:
				_ = client.WriteJSON(map[string]string{"type": "error", "message": "错误的消息格式"})
				return
			}
		}
	}
}

func parseClientMessage(raw []byte, defaultToUserID uint64) (clientMessage, error) {
	text := string(raw)
	if text == "ping" {
		return clientMessage{Type: "ping"}, nil
	}
	if text == "get" {
		if defaultToUserID == 0 {
			return clientMessage{}, errors.New("查询历史消息缺少对端用户 ID")
		}
		return clientMessage{Type: "get", ToUserID: defaultToUserID}, nil
	}
	if strings.HasPrefix(strings.TrimSpace(text), "{") {
		var incoming clientMessage
		if err := json.Unmarshal(raw, &incoming); err != nil {
			return clientMessage{}, fmt.Errorf("解析 WebSocket 消息失败: %w", err)
		}
		if incoming.Type == "ping" {
			return incoming, nil
		}
		if incoming.ToUserID == 0 {
			incoming.ToUserID = defaultToUserID
		}
		if incoming.Type == "get" {
			if incoming.ToUserID == 0 {
				return clientMessage{}, errors.New("查询历史消息缺少对端用户 ID")
			}
			return incoming, nil
		}
		if incoming.Type != "message" || incoming.ToUserID == 0 || strings.TrimSpace(incoming.Content) == "" {
			return clientMessage{}, errors.New("消息类型、对端用户 ID 或内容无效")
		}
		return incoming, nil
	}
	if strings.HasSuffix(text, "\npost") && defaultToUserID != 0 {
		content := strings.TrimSuffix(text, "\npost")
		if strings.TrimSpace(content) == "" {
			return clientMessage{}, errors.New("消息内容为空")
		}
		return clientMessage{Type: "message", ToUserID: defaultToUserID, Content: content}, nil
	}
	return clientMessage{}, errors.New("错误的消息格式")
}
