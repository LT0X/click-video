package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"douyin/config"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

const (
	FavoriteActionExchange      = "favorite"
	FavoriteCounterExchange     = "favorite_counter"
	FavoriteUserQueue           = "favorite_user_rpc"
	FavoriteVideoQueue          = "favorite_video_rpc"
	CacheInvalidationExchange   = "cache_invalidation"
	CacheInvalidationQueue      = "cache_invalidation_gateway"
	CacheInvalidationDelayQueue = "cache_invalidation_delay"
	CacheInvalidationDelay      = 500 * time.Millisecond

	FavoriteCountDelta     = "delta"
	FavoriteCountSnapshot  = "snapshot"
	FavoritePublishTimeout = 5 * time.Second
)

type FavoriteActionEvent struct {
	EventID        uint64 `json:"event_id"`
	ActionSequence uint64 `json:"action_sequence"`
	UserID         uint64 `json:"user_id"`
	AuthorID       uint64 `json:"author_id"`
	VideoID        uint64 `json:"video_id"`
	Cnt            int64  `json:"cnt"`
}

func (event FavoriteActionEvent) Validate() error {
	if event.EventID == 0 || event.UserID == 0 || event.AuthorID == 0 || event.VideoID == 0 {
		return errors.New("点赞消息缺少事件 ID、用户、作者或视频 ID")
	}
	if event.Cnt != 1 && event.Cnt != -1 {
		return fmt.Errorf("点赞消息计数方向无效: %d", event.Cnt)
	}
	return nil
}

func (event FavoriteActionEvent) ValidateForPublish() error {
	if err := event.Validate(); err != nil {
		return err
	}
	if event.ActionSequence == 0 {
		return errors.New("新发布的点赞消息必须包含动作序号")
	}
	return nil
}

type FavoriteCountEvent struct {
	EventID uint64 `json:"event_id"`
	Type    string `json:"type"`
	VideoID uint64 `json:"video_id"`
	Version uint64 `json:"version"`
	Delta   int64  `json:"delta,omitempty"`
	Count   int64  `json:"count,omitempty"`
}

const (
	CacheInvalidationImmediate uint8 = iota
	CacheInvalidationDelayed
)

type CacheInvalidationEvent struct {
	EventID uint64 `json:"event_id"`
	VideoID uint64 `json:"video_id"`
	Phase   uint8  `json:"phase"`
}

func (event CacheInvalidationEvent) Validate() error {
	if event.EventID == 0 || event.VideoID == 0 {
		return errors.New("缓存失效消息缺少事件 ID 或视频 ID")
	}
	if event.Phase != CacheInvalidationImmediate && event.Phase != CacheInvalidationDelayed {
		return fmt.Errorf("缓存失效消息阶段无效: %d", event.Phase)
	}
	return nil
}

func (event FavoriteCountEvent) Validate() error {
	if event.EventID == 0 || event.VideoID == 0 {
		return errors.New("点赞计数消息缺少事件或视频 ID")
	}
	switch event.Type {
	case FavoriteCountDelta:
		if event.Version == 0 || event.Count < 0 {
			return errors.New("点赞增量消息缺少有效版本或绝对计数")
		}
		if event.Delta != 1 && event.Delta != -1 {
			return fmt.Errorf("点赞计数增量无效: %d", event.Delta)
		}
	case FavoriteCountSnapshot:
		if event.Count < 0 {
			return fmt.Errorf("点赞计数快照不可为负数: %d", event.Count)
		}
	default:
		return fmt.Errorf("未知点赞计数消息类型: %q", event.Type)
	}
	return nil
}

type FavoriteEventBroker struct {
	config              config.RabbitMQ
	connection          *amqp.Connection
	publisher           *amqp.Channel
	confirmations       chan amqp.Confirmation
	returns             chan amqp.Return
	nextPublishSequence uint64
	mu                  sync.Mutex
	closed              bool
}

func NewFavoriteEventBroker(cfg config.RabbitMQ) (*FavoriteEventBroker, error) {
	broker := &FavoriteEventBroker{config: cfg}
	broker.mu.Lock()
	err := broker.connectLocked()
	broker.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return broker, nil
}

func (broker *FavoriteEventBroker) connectLocked() error {
	broker.resetConnectionLocked()
	uri := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(broker.config.User, broker.config.Password),
		Host:   net.JoinHostPort(broker.config.Host, broker.config.Port),
	}
	connection, err := amqp.Dial(uri.String())
	if err != nil {
		return fmt.Errorf("连接 RabbitMQ 失败: %w", err)
	}
	publisher, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return fmt.Errorf("创建 RabbitMQ 发布通道失败: %w", err)
	}
	for _, exchange := range []string{FavoriteActionExchange, FavoriteCounterExchange, CacheInvalidationExchange} {
		if err := publisher.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); err != nil {
			_ = publisher.Close()
			_ = connection.Close()
			return fmt.Errorf("声明 RabbitMQ exchange %q 失败: %w", exchange, err)
		}
	}
	for _, item := range []struct {
		queue    string
		exchange string
	}{
		{queue: FavoriteUserQueue, exchange: FavoriteActionExchange},
		{queue: FavoriteVideoQueue, exchange: FavoriteCounterExchange},
		{queue: CacheInvalidationQueue, exchange: CacheInvalidationExchange},
	} {
		queue, err := publisher.QueueDeclare(item.queue, true, false, false, false, nil)
		if err != nil {
			_ = publisher.Close()
			_ = connection.Close()
			return fmt.Errorf("声明 RabbitMQ queue %q 失败: %w", item.queue, err)
		}
		if err := publisher.QueueBind(queue.Name, "", item.exchange, false, nil); err != nil {
			_ = publisher.Close()
			_ = connection.Close()
			return fmt.Errorf("绑定 RabbitMQ queue %q 失败: %w", item.queue, err)
		}
	}
	_, err = publisher.QueueDeclare(CacheInvalidationDelayQueue, true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(CacheInvalidationDelay / time.Millisecond),
		"x-dead-letter-exchange": CacheInvalidationExchange,
	})
	if err != nil {
		_ = publisher.Close()
		_ = connection.Close()
		return fmt.Errorf("声明延迟缓存失效队列失败: %w", err)
	}
	if err := publisher.Confirm(false); err != nil {
		_ = publisher.Close()
		_ = connection.Close()
		return fmt.Errorf("开启 RabbitMQ publisher confirm 失败: %w", err)
	}
	broker.connection = connection
	broker.publisher = publisher
	broker.confirmations = publisher.NotifyPublish(make(chan amqp.Confirmation, 1))
	broker.returns = publisher.NotifyReturn(make(chan amqp.Return, 1))
	broker.nextPublishSequence = 1
	return nil
}

func (broker *FavoriteEventBroker) resetConnectionLocked() {
	if broker.publisher != nil {
		_ = broker.publisher.Close()
		broker.publisher = nil
	}
	if broker.connection != nil {
		_ = broker.connection.Close()
		broker.connection = nil
	}
	broker.confirmations = nil
	broker.returns = nil
	broker.nextPublishSequence = 0
}

func (broker *FavoriteEventBroker) consumerChannel() (*amqp.Channel, error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return nil, errors.New("RabbitMQ broker 已关闭")
	}
	if broker.connection == nil || broker.connection.IsClosed() || broker.publisher == nil || broker.publisher.IsClosed() {
		if err := broker.connectLocked(); err != nil {
			return nil, err
		}
	}
	channel, err := broker.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("创建 RabbitMQ 消费通道失败: %w", err)
	}
	return channel, nil
}

func (broker *FavoriteEventBroker) Close() error {
	if broker == nil {
		return nil
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return nil
	}
	broker.closed = true
	var channelErr, connectionErr error
	if broker.publisher != nil {
		channelErr = broker.publisher.Close()
	}
	if broker.connection != nil {
		connectionErr = broker.connection.Close()
	}
	if channelErr != nil {
		return channelErr
	}
	return connectionErr
}

func (broker *FavoriteEventBroker) PublishFavoriteCount(event FavoriteCountEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return broker.publish(FavoriteCounterExchange, event.EventID, event)
}

func (broker *FavoriteEventBroker) PublishFavoriteAction(event FavoriteActionEvent) error {
	if err := event.ValidateForPublish(); err != nil {
		return err
	}
	return broker.publish(FavoriteActionExchange, event.EventID, event)
}

func (broker *FavoriteEventBroker) PublishCacheInvalidation(event CacheInvalidationEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if event.Phase == CacheInvalidationDelayed {
		return broker.publishTo("", CacheInvalidationDelayQueue, event.EventID, event)
	}
	return broker.publish(CacheInvalidationExchange, event.EventID, event)
}

func (broker *FavoriteEventBroker) publish(exchange string, eventID uint64, event interface{}) error {
	return broker.publishTo(exchange, "", eventID, event)
}

func (broker *FavoriteEventBroker) publishTo(exchange, routingKey string, eventID uint64, event interface{}) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("序列化 RabbitMQ 消息失败: %w", err)
	}
	messageID := strconv.FormatUint(eventID, 10)
	return broker.publishRaw(exchange, routingKey, messageID, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    messageID,
		Body:         body,
	})
}

func (broker *FavoriteEventBroker) publishRaw(exchange, routingKey, eventID string, mandatory bool, message amqp.Publishing) error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return errors.New("RabbitMQ broker 已关闭")
	}
	if broker.publisher == nil || broker.publisher.IsClosed() {
		if err := broker.connectLocked(); err != nil {
			return err
		}
	}
	err := broker.publisher.Publish(exchange, routingKey, mandatory, false, message)
	if err != nil {
		broker.resetConnectionLocked()
		return fmt.Errorf("发布 RabbitMQ 消息到 %q 失败: %w", exchange, err)
	}
	expectedSequence := broker.nextPublishSequence
	broker.nextPublishSequence++
	select {
	case confirmation, ok := <-broker.confirmations:
		if !ok {
			broker.resetConnectionLocked()
			return fmt.Errorf("RabbitMQ publisher confirm 通道已关闭")
		}
		if confirmation.DeliveryTag != expectedSequence {
			broker.resetConnectionLocked()
			return fmt.Errorf("RabbitMQ publisher confirm 序号不匹配: got=%d want=%d", confirmation.DeliveryTag, expectedSequence)
		}
		if !confirmation.Ack {
			return fmt.Errorf("RabbitMQ 拒绝确认 exchange=%q event_id=%s", exchange, eventID)
		}
	case <-time.After(FavoritePublishTimeout):
		// 超时后关闭通道，避免迟到的确认被错误地匹配到下一条消息。
		broker.resetConnectionLocked()
		return fmt.Errorf("等待 RabbitMQ 确认超时 exchange=%q event_id=%s", exchange, eventID)
	}
	if mandatory {
		select {
		case returned := <-broker.returns:
			return fmt.Errorf("RabbitMQ 消息未路由到队列 exchange=%q event_id=%s reply=%s", exchange, eventID, returned.ReplyText)
		default:
		}
	}
	return nil
}

func (broker *FavoriteEventBroker) ConsumeFavoriteActions(ctx context.Context, handler func(context.Context, FavoriteActionEvent) error) error {
	return broker.consume(ctx, FavoriteActionExchange, FavoriteUserQueue, func(body []byte) error {
		var event FavoriteActionEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return invalidFavoriteMessage(fmt.Errorf("解析点赞动作消息失败: %w", err))
		}
		if err := event.Validate(); err != nil {
			return invalidFavoriteMessage(err)
		}
		return handler(ctx, event)
	})
}

func (broker *FavoriteEventBroker) ConsumeFavoriteCounts(ctx context.Context, handler func(context.Context, FavoriteCountEvent) error) error {
	return broker.consume(ctx, FavoriteCounterExchange, FavoriteVideoQueue, func(body []byte) error {
		var event FavoriteCountEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return invalidFavoriteMessage(fmt.Errorf("解析点赞计数消息失败: %w", err))
		}
		if err := event.Validate(); err != nil {
			return invalidFavoriteMessage(err)
		}
		return handler(ctx, event)
	})
}

func (broker *FavoriteEventBroker) ConsumeCacheInvalidations(ctx context.Context, handler func(context.Context, CacheInvalidationEvent) error) error {
	return broker.consume(ctx, CacheInvalidationExchange, CacheInvalidationQueue, func(body []byte) error {
		var event CacheInvalidationEvent
		if err := json.Unmarshal(body, &event); err != nil {
			return invalidFavoriteMessage(fmt.Errorf("解析缓存失效消息失败: %w", err))
		}
		if err := event.Validate(); err != nil {
			return invalidFavoriteMessage(err)
		}
		return handler(ctx, event)
	})
}

func (broker *FavoriteEventBroker) RunFavoriteActions(ctx context.Context, handler func(context.Context, FavoriteActionEvent) error) {
	broker.runConsumer(ctx, func() error { return broker.ConsumeFavoriteActions(ctx, handler) })
}

func (broker *FavoriteEventBroker) RunFavoriteCounts(ctx context.Context, handler func(context.Context, FavoriteCountEvent) error) {
	broker.runConsumer(ctx, func() error { return broker.ConsumeFavoriteCounts(ctx, handler) })
}

func (broker *FavoriteEventBroker) RunCacheInvalidations(ctx context.Context, handler func(context.Context, CacheInvalidationEvent) error) {
	broker.runConsumer(ctx, func() error { return broker.ConsumeCacheInvalidations(ctx, handler) })
}

func (broker *FavoriteEventBroker) runConsumer(ctx context.Context, consume func() error) {
	for ctx.Err() == nil {
		if err := consume(); err != nil && ctx.Err() == nil {
			zap.L().Error("RabbitMQ consumer 中断，稍后重连", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}
	}
}

func (broker *FavoriteEventBroker) consume(ctx context.Context, exchange, queue string, handler func([]byte) error) error {
	channel, err := broker.consumerChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := channel.Qos(50, 0, false); err != nil {
		return fmt.Errorf("设置 RabbitMQ 消费限流失败: %w", err)
	}
	if err := channel.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("声明 RabbitMQ exchange %q 失败: %w", exchange, err)
	}
	q, err := channel.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("声明 RabbitMQ queue %q 失败: %w", queue, err)
	}
	if err := channel.QueueBind(q.Name, "", exchange, false, nil); err != nil {
		return fmt.Errorf("绑定 RabbitMQ queue %q 失败: %w", queue, err)
	}
	deliveries, err := channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("启动 RabbitMQ consumer %q 失败: %w", queue, err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("RabbitMQ consumer %q 已关闭", queue)
			}
			if err := handler(delivery.Body); err != nil {
				zap.L().Error("处理 RabbitMQ 消息失败", zap.String("queue", queue), zap.Error(err))
				var invalid invalidFavoriteMessageError
				if errors.As(err, &invalid) {
					_ = delivery.Ack(false)
					continue
				}
				if nackErr := delivery.Nack(false, true); nackErr != nil {
					return fmt.Errorf("重新入队 RabbitMQ 消息失败: %w", nackErr)
				}
				continue
			}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("确认 RabbitMQ 消息失败: %w", err)
			}
		}
	}
}

type invalidFavoriteMessageError struct{ err error }

func (e invalidFavoriteMessageError) Error() string { return e.err.Error() }
func (e invalidFavoriteMessageError) Unwrap() error { return e.err }

func invalidFavoriteMessage(err error) error {
	return invalidFavoriteMessageError{err: err}
}

// DiscardFavoriteMessage 将永久无效的业务消息标记为可确认丢弃。
func DiscardFavoriteMessage(err error) error {
	return invalidFavoriteMessage(err)
}
