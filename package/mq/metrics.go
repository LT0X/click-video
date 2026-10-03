package mq

import (
	"context"
	"errors"
	"time"

	"douyin/package/metrics"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// QueueDepthSample 保存一个固定队列的最近采样结果。
type QueueDepthSample struct {
	Queue    string
	Messages int
	Err      error
}

type queueInspectionChannel interface {
	InspectQueueDepth(queue string) (int, error)
	Close() error
}

type amqpQueueInspectionChannel struct {
	channel *amqp.Channel
}

func (channel amqpQueueInspectionChannel) InspectQueueDepth(queue string) (int, error) {
	info, err := channel.channel.QueueInspect(queue)
	return info.Messages, err
}

func (channel amqpQueueInspectionChannel) Close() error {
	return channel.channel.Close()
}

var monitoredQueueNames = []string{
	FavoriteUserQueue,
	FavoriteVideoQueue,
	CacheInvalidationQueue,
	commentQueue,
	commentRetryQueue,
	commentDeadLetterQueue,
}

// InspectQueueDepths 使用独立 Channel 检查固定队列；锁保护连接重建和 Channel 创建。
// QueueInspect 失败时保留逐队列错误，调用方不会用零值覆盖上次成功的 Gauge。
func (broker *FavoriteEventBroker) InspectQueueDepths() []QueueDepthSample {
	if broker == nil {
		return failedQueueDepthSamples(errors.New("RabbitMQ broker 未初始化"))
	}

	broker.mu.Lock()
	if broker.closed {
		broker.mu.Unlock()
		return failedQueueDepthSamples(errors.New("RabbitMQ broker 已关闭"))
	}
	if broker.connection == nil || broker.connection.IsClosed() || broker.publisher == nil || broker.publisher.IsClosed() {
		if err := broker.connectLocked(); err != nil {
			broker.mu.Unlock()
			return failedQueueDepthSamples(err)
		}
	}
	connection := broker.connection
	broker.mu.Unlock()
	if connection == nil {
		return failedQueueDepthSamples(errors.New("RabbitMQ connection 未初始化"))
	}

	// 每个被动检查使用独立 Channel；不存在的队列会关闭自己的 Channel。
	// 网络 RPC 在 broker 锁外执行，避免慢采样阻塞业务发布。
	return inspectQueueDepthsWithChannels(func() (queueInspectionChannel, error) {
		channel, err := connection.Channel()
		if err != nil {
			return nil, err
		}
		return amqpQueueInspectionChannel{channel: channel}, nil
	})
}

func inspectQueueDepths(inspect func(string) (int, error)) []QueueDepthSample {
	samples := make([]QueueDepthSample, 0, len(monitoredQueueNames))
	for _, queue := range monitoredQueueNames {
		messages, err := inspect(queue)
		samples = append(samples, QueueDepthSample{Queue: queue, Messages: messages, Err: err})
	}
	return samples
}

func inspectQueueDepthsWithChannels(openChannel func() (queueInspectionChannel, error)) []QueueDepthSample {
	samples := make([]QueueDepthSample, 0, len(monitoredQueueNames))
	for _, queue := range monitoredQueueNames {
		channel, err := openChannel()
		if err != nil {
			samples = append(samples, QueueDepthSample{Queue: queue, Err: err})
			continue
		}

		messages, inspectErr := channel.InspectQueueDepth(queue)
		if err := channel.Close(); err != nil {
			zap.L().Warn("关闭 RabbitMQ 队列采样 Channel 失败", zap.String("queue", queue), zap.Error(err))
		}
		samples = append(samples, QueueDepthSample{Queue: queue, Messages: messages, Err: inspectErr})
	}
	return samples
}

func failedQueueDepthSamples(err error) []QueueDepthSample {
	return inspectQueueDepths(func(string) (int, error) {
		return 0, err
	})
}

// applyQueueDepthSamples 对失败样本只增加错误计数，保留上次成功的 Gauge。
func applyQueueDepthSamples(registry *metrics.Registry, samples []QueueDepthSample) {
	for _, sample := range samples {
		if sample.Err != nil {
			registry.ObserveRabbitMQQueueInspectionError(sample.Queue)
			zap.L().Warn("采样 RabbitMQ 队列积压失败", zap.String("queue", sample.Queue), zap.Error(sample.Err))
			continue
		}
		registry.SetRabbitMQQueueDepth(sample.Queue, sample.Messages)
	}
}

// RunQueueDepthMonitor 周期性采样 RabbitMQ 积压，启动时先采样一次。
func RunQueueDepthMonitor(ctx context.Context, broker *FavoriteEventBroker, interval time.Duration) {
	runQueueDepthMonitor(ctx, metrics.Default, broker.InspectQueueDepths, interval)
}

func runQueueDepthMonitor(ctx context.Context, registry *metrics.Registry, inspect func() []QueueDepthSample, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if ctx.Err() != nil {
		return
	}
	sample := func() {
		applyQueueDepthSamples(registry, inspect())
	}
	sample()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sample()
		}
	}
}

func deliveryLag(timestamp, now time.Time) (time.Duration, bool) {
	if timestamp.IsZero() || now.Before(timestamp) {
		return 0, false
	}
	return now.Sub(timestamp), true
}

func recordConsumerLag(registry *metrics.Registry, queue string, timestamp, now time.Time) {
	if timestamp.IsZero() {
		registry.ObserveRabbitMQMissingTimestamp(queue)
		return
	}
	lag, ok := deliveryLag(timestamp, now)
	if !ok {
		return
	}
	registry.ObserveRabbitMQConsumerLag(queue, lag)
}

// recordPublishingTimestamp 为历史调用方补足初次发布时间，方便消费者统一统计排队延迟。
func recordPublishingTimestamp(message *amqp.Publishing, now time.Time) {
	if message.Timestamp.IsZero() {
		message.Timestamp = now.UTC()
	}
}
