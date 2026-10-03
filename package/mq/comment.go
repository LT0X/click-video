package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"douyin/database"
	"douyin/model"
	"douyin/rpc/video/video"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	commentExchange           = "comment"
	commentQueue              = "comment_writer"
	commentRetryExchange      = "comment_retry"
	commentRetryQueue         = "comment_retry_1s"
	commentDeadLetterExchange = "comment_dead_letter"
	commentDeadLetterQueue    = "comment_dead_letter"
	commentRetryRoutingKey    = "retry"
	commentMaxRetries         = 3
	commentRetryDelay         = time.Second
)

func initComment() {
	if favoriteBroker == nil {
		zap.L().Error("初始化评论消息失败: RabbitMQ broker 为空")
		return
	}
	if err := favoriteBroker.declareCommentTopology(); err != nil {
		zap.L().Error("初始化评论持久队列失败", zap.Error(err))
		return
	}
	go runCommentConsumer(context.Background(), favoriteBroker)
}

func (broker *FavoriteEventBroker) declareCommentTopology() error {
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
	_, err := declareCommentQueue(broker.publisher)
	return err
}

func SendCommentMessage(msg *model.Comment) error {
	if err := validateCommentMessage(msg); err != nil {
		return err
	}
	if favoriteBroker == nil {
		return errors.New("RabbitMQ 评论发布 broker 不可用")
	}
	msgJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化评论消息失败: %w", err)
	}
	messageID := strconv.FormatUint(msg.ID, 10)
	if err := favoriteBroker.publishRaw(commentExchange, "", messageID, true, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "application/json",
		MessageId:    messageID,
		Body:         msgJSON,
	}); err != nil {
		return fmt.Errorf("发布评论消息失败: %w", err)
	}
	return nil
}

func runCommentConsumer(ctx context.Context, broker *FavoriteEventBroker) {
	if broker == nil {
		zap.L().Error("评论消费者未启动: RabbitMQ broker 尚未初始化")
		return
	}
	broker.runConsumer(ctx, func() error {
		return consumeComments(ctx, broker)
	})
}

func consumeComments(ctx context.Context, broker *FavoriteEventBroker) error {
	channel, err := broker.consumerChannel()
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := channel.Qos(50, 0, false); err != nil {
		return fmt.Errorf("设置评论消费者限流失败: %w", err)
	}
	queue, err := declareCommentQueue(channel)
	if err != nil {
		return err
	}
	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("启动评论消费者失败: %w", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return errors.New("评论消费者通道已关闭")
			}
			discard, err := processCommentPayload(delivery.Body, persistComment)
			if err != nil {
				zap.L().Error("处理评论消息失败", zap.Error(err))
				if discard || isPermanentCommentError(err) || commentRetryCount(delivery.Headers) >= commentMaxRetries {
					if deadErr := publishCommentDeadLetter(broker, delivery, err); deadErr != nil {
						zap.L().Error("评论死信投递失败，将消息送入延迟重试队列", zap.Error(deadErr))
						if nackErr := delivery.Nack(false, false); nackErr != nil {
							return fmt.Errorf("重试评论死信投递失败: %w", nackErr)
						}
						continue
					}
					if ackErr := delivery.Ack(false); ackErr != nil {
						return fmt.Errorf("确认已转入死信队列的评论消息失败: %w", ackErr)
					}
					continue
				}
				if nackErr := delivery.Nack(false, false); nackErr != nil {
					return fmt.Errorf("将评论消息送入延迟重试队列失败: %w", nackErr)
				}
				continue
			}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("确认评论消息失败: %w", err)
			}
		}
	}
}

func declareCommentQueue(channel *amqp.Channel) (amqp.Queue, error) {
	if err := channel.ExchangeDeclare(commentExchange, "fanout", true, false, false, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("声明 comment exchange 失败: %w", err)
	}
	if err := channel.ExchangeDeclare(commentRetryExchange, "direct", true, false, false, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("声明评论重试 exchange 失败: %w", err)
	}
	if err := channel.ExchangeDeclare(commentDeadLetterExchange, "fanout", true, false, false, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("声明评论死信 exchange 失败: %w", err)
	}
	queue, err := channel.QueueDeclare(commentQueue, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    commentRetryExchange,
		"x-dead-letter-routing-key": commentRetryRoutingKey,
	})
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("声明评论持久队列失败: %w", err)
	}
	if err := channel.QueueBind(queue.Name, "", commentExchange, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("绑定评论持久队列失败: %w", err)
	}
	retryQueue, err := channel.QueueDeclare(commentRetryQueue, true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(commentRetryDelay / time.Millisecond),
		"x-dead-letter-exchange": commentExchange,
	})
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("声明评论延迟重试队列失败: %w", err)
	}
	if err := channel.QueueBind(retryQueue.Name, commentRetryRoutingKey, commentRetryExchange, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("绑定评论延迟重试队列失败: %w", err)
	}
	deadLetterQueue, err := channel.QueueDeclare(commentDeadLetterQueue, true, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("声明评论死信队列失败: %w", err)
	}
	if err := channel.QueueBind(deadLetterQueue.Name, "", commentDeadLetterExchange, false, nil); err != nil {
		return amqp.Queue{}, fmt.Errorf("绑定评论死信队列失败: %w", err)
	}
	return queue, nil
}

func decodeCommentPayload(body []byte) (*model.Comment, error) {
	var comment model.Comment
	if err := json.Unmarshal(body, &comment); err != nil {
		return nil, fmt.Errorf("解析评论消息失败: %w", err)
	}
	if err := validateCommentMessage(&comment); err != nil {
		return nil, err
	}
	return &comment, nil
}

func validateCommentMessage(comment *model.Comment) error {
	if comment == nil || comment.ID == 0 || comment.VideoID == 0 || comment.UserID == 0 || comment.CreatedTime.IsZero() {
		return errors.New("评论消息缺少评论 ID、视频 ID、用户 ID 或创建时间")
	}
	contentLength := utf8.RuneCountInString(comment.Content)
	if contentLength == 0 || contentLength > 255 {
		return errors.New("评论内容必须为 1 至 255 个字符")
	}
	return nil
}

func isPermanentCommentError(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		code := status.Code(current)
		if code == codes.InvalidArgument || code == codes.NotFound {
			return true
		}
	}
	return false
}

func commentRetryCount(headers amqp.Table) int64 {
	var entries []interface{}
	switch deaths := headers["x-death"].(type) {
	case []interface{}:
		entries = deaths
	case []amqp.Table:
		for _, death := range deaths {
			entries = append(entries, death)
		}
	default:
		return 0
	}
	for _, value := range entries {
		death, ok := value.(amqp.Table)
		if !ok || death["queue"] != commentQueue || death["reason"] != "rejected" {
			continue
		}
		switch count := death["count"].(type) {
		case int64:
			return count
		case int32:
			return int64(count)
		case uint64:
			return int64(count)
		case int:
			return int64(count)
		}
	}
	return 0
}

func publishCommentDeadLetter(broker *FavoriteEventBroker, delivery amqp.Delivery, cause error) error {
	if broker == nil {
		return errors.New("评论死信 broker 未初始化")
	}
	headers := make(amqp.Table, len(delivery.Headers)+1)
	for key, value := range delivery.Headers {
		headers[key] = value
	}
	headers["x-comment-error"] = cause.Error()
	messageID := delivery.MessageId
	if messageID == "" {
		messageID = "unknown"
	}
	return broker.publishRaw(commentDeadLetterExchange, "", messageID, true, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  delivery.ContentType,
		MessageId:    messageID,
		Headers:      headers,
		Body:         append([]byte(nil), delivery.Body...),
	})
}

// processCommentPayload 将格式错误的毒消息标记为可丢弃，业务处理失败则交由队列重试。
func processCommentPayload(body []byte, handler func(*model.Comment) error) (bool, error) {
	comment, err := decodeCommentPayload(body)
	if err != nil {
		return true, err
	}
	if err := handler(comment); err != nil {
		return false, err
	}
	return false, nil
}

func processCommentPayloads(bodies [][]byte, handler func(commentID, videoID uint64) error) error {
	for _, body := range bodies {
		discard, err := processCommentPayload(body, func(comment *model.Comment) error {
			return handler(comment.ID, comment.VideoID)
		})
		if err != nil && !discard {
			return err
		}
	}
	return nil
}

func persistComment(comment *model.Comment) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := database.RPC.VideoRpc.CommentAdd(ctx, &video.CommentAddRequest{
		Comment: model.TransformRPCComment(comment),
	})
	if err != nil {
		return fmt.Errorf("VideoRpc.CommentAdd 失败: %w", err)
	}
	return nil
}
