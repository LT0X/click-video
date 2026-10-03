package logic

import (
	"context"
	"errors"
	"fmt"
	"time"

	"douyin/package/mq"
	usercache "douyin/rpc/user/internal/cache"
	"douyin/rpc/user/internal/model"
	"douyin/rpc/user/internal/svc"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	favoriteOutboxPollInterval = time.Second
	favoriteOutboxBatchSize    = 50
)

// favoriteCountOutboxEvent 将事务内保存的计数结果转换为可幂等重放的消息。
func favoriteCountOutboxEvent(row model.FavoriteCountOutbox) mq.FavoriteCountEvent {
	return mq.FavoriteCountEvent{
		EventID: row.EventID,
		Type:    mq.FavoriteCountDelta,
		VideoID: row.VideoID,
		Version: row.CountVersion,
		Delta:   row.Delta,
		Count:   row.FavoriteCount,
	}
}

// PublishPendingFavoriteCountEvents 在持有 Outbox 行锁期间发布并等待 broker confirm。
// 若确认后标记失败，事务回滚后会重发同一 EventID，video.rpc 按版本和事件 ID 幂等处理。
func PublishPendingFavoriteCountEvents(ctx context.Context, svcCtx *svc.ServiceContext, broker *mq.FavoriteEventBroker, limit int) (int, error) {
	if limit <= 0 {
		limit = favoriteOutboxBatchSize
	}
	published := 0
	for published < limit {
		didPublish := false
		err := svcCtx.DBList.Mysql.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var row model.FavoriteCountOutbox
			err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
				Where("published_at IS NULL").Order("id ASC").Take(&row).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("读取待发布点赞计数 Outbox 失败: %w", err)
			}

			// 缓存删除是幂等操作；失败时回滚锁行事务，留待下一轮可靠重试。
			if err := usercache.FavoriteAction(row.UserID, row.AuthorID); err != nil {
				return fmt.Errorf("清除点赞用户缓存 event_id=%d 失败: %w", row.EventID, err)
			}
			if err := broker.PublishFavoriteCount(favoriteCountOutboxEvent(row)); err != nil {
				return fmt.Errorf("发布点赞计数 Outbox event_id=%d 失败: %w", row.EventID, err)
			}
			now := time.Now()
			if err := tx.Model(&row).Update("published_at", now).Error; err != nil {
				return fmt.Errorf("标记点赞计数 Outbox event_id=%d 已发布失败: %w", row.EventID, err)
			}
			didPublish = true
			return nil
		})
		if err != nil {
			return published, err
		}
		if !didPublish {
			return published, nil
		}
		published++
	}
	return published, nil
}

// RunFavoriteCountOutboxPublisher 以短周期扫描事务 Outbox，避免数据库提交与 MQ 发布之间丢失事件。
func RunFavoriteCountOutboxPublisher(ctx context.Context, svcCtx *svc.ServiceContext, broker *mq.FavoriteEventBroker) {
	ticker := time.NewTicker(favoriteOutboxPollInterval)
	defer ticker.Stop()
	for {
		if _, err := PublishPendingFavoriteCountEvents(ctx, svcCtx, broker, favoriteOutboxBatchSize); err != nil && ctx.Err() == nil {
			zap.L().Error("发布点赞计数 Outbox 失败，将重试", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
