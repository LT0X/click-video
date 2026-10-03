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
		return inspectQueueDepths(func(string) (int, error) {
			return 0, errors.New("RabbitMQ broker 未初始化")
		})
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return inspectQueueDepths(func(string) (int, error) {
			return 0, errors.New("RabbitMQ broker 已关闭")
		})
	}
	if broker.connection == nil || broker.connection.IsClosed() || broker.publisher == nil || broker.publisher.IsClosed() {
		if err := broker.connectLocked(); err != nil {
			return inspectQueueDepths(func(string) (int, error) {
				return 0, err
			})
		}
	}
	channel, err := broker.connection.Channel()
	if err != nil {
		return inspectQueueDepths(func(string) (int, error) {
			return 0, err
		})
	}
	defer func() {
		if err := channel.Close(); err != nil {
			zap.L().Warn("关闭 RabbitMQ 队列采样 Channel 失败", zap.Error(err))
		}
	}()
	return inspectQueueDepths(func(queue string) (int, error) {
		info, err := channel.QueueInspect(queue)
		if err != nil {
			return 0, err
		}
		return info.Messages, nil
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
