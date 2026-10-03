package chat

import (
	"context"

	"go.uber.org/zap"
)

type Enqueuer interface {
	Enqueue(context.Context, Message) error
}

type RouteResolver interface {
	Lookup(context.Context, uint64) (string, error)
}

type RemotePusher interface {
	Push(context.Context, string, Message) error
}

// Dispatcher 先持久缓冲消息，再尽力实时推送；在线路由故障不会抹掉已接受的消息。
type Dispatcher struct {
	selfAddress string
	registry    *Registry
	queue       Enqueuer
	routes      RouteResolver
	remote      RemotePusher
}

func NewDispatcher(selfAddress string, registry *Registry, queue Enqueuer, routes RouteResolver, remote RemotePusher) *Dispatcher {
	return &Dispatcher{selfAddress: selfAddress, registry: registry, queue: queue, routes: routes, remote: remote}
}

func (d *Dispatcher) Send(ctx context.Context, msg Message) error {
	if msg.EventID == "" || msg.FromUserID == 0 || msg.ToUserID == 0 || msg.Content == "" {
		return ErrInvalidMessage
	}
	if err := d.queue.Enqueue(ctx, msg); err != nil {
		return err
	}

	address, err := d.routes.Lookup(ctx, msg.ToUserID)
	if err != nil {
		zap.L().Warn("读取聊天在线路由失败，消息已进入持久缓冲", zap.Uint64("user_id", msg.ToUserID), zap.Error(err))
		return nil
	}
	if address == "" {
		return nil // 用户离线，消息留在 Redis Stream 等待落库和上线后拉取。
	}
	if address == d.selfAddress {
		if err := d.registry.Deliver(msg.ToUserID, msg); err != nil {
			zap.L().Warn("本机 WebSocket 实时投递失败，消息已进入持久缓冲", zap.Uint64("user_id", msg.ToUserID), zap.Error(err))
		}
		return nil
	}
	if err := d.remote.Push(ctx, address, msg); err != nil {
		zap.L().Warn("跨节点 WebSocket 实时投递失败，消息已进入持久缓冲", zap.String("node", address), zap.Error(err))
	}
	return nil
}
