package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

const (
	MessageStreamKey      = "chat:message:buffer"
	MessageDeadLetterKey  = "chat:message:dead_letter"
	MessageConsumerGroup  = "chat-message-writers"
	DefaultMessageBatch   = 50
	DefaultMessageFlush   = 250 * time.Millisecond
	messageClaimMinIdle   = 15 * time.Second
	messageStreamReadWait = 100 * time.Millisecond
)

type MessageBatchPersister func(context.Context, []Message) error

type streamEntry struct {
	id      string
	message Message
}

// StreamBuffer 用 Redis Stream 保存待落库消息。只有 contact.rpc 批量写库成功后才 ACK，
// 这样数据库故障或网关重启时消息仍会留在消费组待重试记录中。
type StreamBuffer struct {
	client      *redis.Client
	consumer    string
	persist     MessageBatchPersister
	batchSize   int64
	flushWindow time.Duration
}

func NewStreamBuffer(client *redis.Client, consumer string, persist MessageBatchPersister) *StreamBuffer {
	if consumer == "" {
		host, _ := os.Hostname()
		consumer = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return &StreamBuffer{client: client, consumer: consumer, persist: persist, batchSize: DefaultMessageBatch, flushWindow: DefaultMessageFlush}
}

func (b *StreamBuffer) Enqueue(ctx context.Context, msg Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b == nil || b.client == nil {
		return errors.New("聊天 Redis Stream 未初始化")
	}
	if err := validateMessage(msg); err != nil {
		return err
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化聊天消息失败: %w", err)
	}
	if err := b.client.XAdd(&redis.XAddArgs{Stream: MessageStreamKey, Values: map[string]interface{}{"payload": string(payload)}}).Err(); err != nil {
		return fmt.Errorf("写入聊天 Redis Stream 失败: %w", err)
	}
	return nil
}

func DecodeStreamMessage(raw string) (Message, error) {
	var msg Message
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return Message{}, fmt.Errorf("解析聊天消息 JSON 失败: %w", err)
	}
	if err := validateMessage(msg); err != nil {
		return Message{}, err
	}
	return msg, nil
}

func validateMessage(msg Message) error {
	if msg.EventID == "" || msg.FromUserID == 0 || msg.ToUserID == 0 || strings.TrimSpace(msg.Content) == "" || msg.CreateTime <= 0 {
		return errors.New("聊天消息缺少 event_id、用户 ID、内容或时间")
	}
	return nil
}

func (b *StreamBuffer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := b.ensureGroup(); err != nil {
			zap.L().Error("创建聊天消息消费组失败，稍后重试", zap.Error(err))
			if !waitForRetry(ctx) {
				return
			}
			continue
		}
		break
	}
	if ctx.Err() != nil {
		return
	}
	batch := make([]streamEntry, 0, b.batchSize)
	firstMessageAt := time.Time{}
	for ctx.Err() == nil {
		claimed, err := b.claimStale()
		if err != nil {
			zap.L().Warn("领取超时的聊天消息失败", zap.Error(err))
		}
		wasEmpty := len(batch) == 0
		batch = b.appendMessages(batch, claimed)
		if wasEmpty && len(batch) > 0 {
			firstMessageAt = time.Now()
		}

		if int64(len(batch)) < b.batchSize {
			fresh, readErr := b.readNew(b.batchSize - int64(len(batch)))
			if readErr != nil && !errors.Is(readErr, redis.Nil) {
				zap.L().Warn("读取聊天消息缓冲失败", zap.Error(readErr))
				if !waitForRetry(ctx) {
					return
				}
				continue
			} else if len(fresh) > 0 {
				if len(batch) == 0 {
					firstMessageAt = time.Now()
				}
				batch = b.appendMessages(batch, fresh)
			}
		}

		if len(batch) == 0 {
			firstMessageAt = time.Time{}
			continue
		}
		if !ShouldFlushBatch(len(batch), firstMessageAt, time.Now(), int(b.batchSize), b.flushWindow) {
			continue
		}
		if err := b.persistAndAck(ctx, batch); err != nil {
			zap.L().Error("批量落库聊天消息失败，保留消息等待重试", zap.Int("count", len(batch)), zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		batch = batch[:0]
		firstMessageAt = time.Time{}
	}
}

func ShouldFlushBatch(count int, firstMessageAt, now time.Time, batchSize int, flushWindow time.Duration) bool {
	if count <= 0 {
		return false
	}
	return count >= batchSize || (!firstMessageAt.IsZero() && now.Sub(firstMessageAt) >= flushWindow)
}

func (b *StreamBuffer) ensureGroup() error {
	err := b.client.XGroupCreateMkStream(MessageStreamKey, MessageConsumerGroup, "0").Err()
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (b *StreamBuffer) claimStale() ([]redis.XMessage, error) {
	pending, err := b.client.XPendingExt(&redis.XPendingExtArgs{
		Stream: MessageStreamKey, Group: MessageConsumerGroup, Start: "-", End: "+", Count: b.batchSize,
	}).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(pending))
	for _, item := range pending {
		if item.Idle >= messageClaimMinIdle {
			ids = append(ids, item.Id)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return b.client.XClaim(&redis.XClaimArgs{
		Stream: MessageStreamKey, Group: MessageConsumerGroup, Consumer: b.consumer,
		MinIdle: messageClaimMinIdle, Messages: ids,
	}).Result()
}

func (b *StreamBuffer) readNew(count int64) ([]redis.XMessage, error) {
	streams, err := b.client.XReadGroup(&redis.XReadGroupArgs{
		Group: MessageConsumerGroup, Consumer: b.consumer, Streams: []string{MessageStreamKey, ">"},
		Count: count, Block: messageStreamReadWait,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, nil
	}
	return streams[0].Messages, nil
}

func (b *StreamBuffer) appendMessages(batch []streamEntry, entries []redis.XMessage) []streamEntry {
	for _, entry := range entries {
		alreadyQueued := false
		for _, queued := range batch {
			if queued.id == entry.ID {
				alreadyQueued = true
				break
			}
		}
		if alreadyQueued {
			continue
		}
		raw := streamValue(entry.Values["payload"])
		msg, err := DecodeStreamMessage(raw)
		if err != nil {
			if quarantineErr := b.quarantine(entry.ID, raw, err); quarantineErr != nil {
				zap.L().Error("隔离无效聊天消息失败，消息仍保留待处理", zap.String("stream_id", entry.ID), zap.Error(quarantineErr))
				continue
			}
			zap.L().Error("无效聊天消息已写入死信 Stream", zap.String("stream_id", entry.ID), zap.Error(err))
			continue
		}
		batch = append(batch, streamEntry{id: entry.ID, message: msg})
	}
	return batch
}

func waitForRetry(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Second):
		return true
	}
}

func streamValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func (b *StreamBuffer) quarantine(streamID, payload string, cause error) error {
	if err := b.client.XAdd(&redis.XAddArgs{Stream: MessageDeadLetterKey, Values: map[string]interface{}{
		"source_id": streamID, "payload": payload, "error": cause.Error(),
	}}).Err(); err != nil {
		return fmt.Errorf("写入聊天死信 Stream 失败: %w", err)
	}
	return b.ackAndDelete([]string{streamID})
}

func (b *StreamBuffer) persistAndAck(ctx context.Context, batch []streamEntry) error {
	messages := make([]Message, len(batch))
	ids := make([]string, len(batch))
	for i, entry := range batch {
		messages[i], ids[i] = entry.message, entry.id
	}
	return PersistThenAcknowledge(ctx, messages, b.persist, func() error { return b.ackAndDelete(ids) })
}

func (b *StreamBuffer) ackAndDelete(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	pipe := b.client.TxPipeline()
	pipe.XAck(MessageStreamKey, MessageConsumerGroup, ids...)
	pipe.XDel(MessageStreamKey, ids...)
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("确认聊天 Stream 消息失败: %w", err)
	}
	return nil
}

// PersistThenAcknowledge 保证消费确认严格晚于批量数据库写入。
func PersistThenAcknowledge(ctx context.Context, messages []Message, persist MessageBatchPersister, acknowledge func() error) error {
	if len(messages) == 0 {
		return nil
	}
	if persist == nil || acknowledge == nil {
		return errors.New("聊天消息持久化或确认函数未初始化")
	}
	if err := persist(ctx, messages); err != nil {
		return fmt.Errorf("contact.rpc 批量持久化消息失败: %w", err)
	}
	if err := acknowledge(); err != nil {
		return fmt.Errorf("消息已落库但 Stream 确认失败: %w", err)
	}
	return nil
}

// PendingCount 用于监控聊天缓冲队列的积压量。
func (b *StreamBuffer) PendingCount() (int64, error) {
	if b == nil || b.client == nil {
		return 0, errors.New("聊天 Redis Stream 未初始化")
	}
	pending, err := b.client.XPending(MessageStreamKey, MessageConsumerGroup).Result()
	if err != nil {
		return 0, err
	}
	return pending.Count, nil
}
