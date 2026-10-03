package mq

import (
	"context"
	"errors"
	"time"

	eventmq "douyin/package/mq"
	"douyin/rpc/video/internal/model"
	"douyin/rpc/video/internal/svc"

	"go.uber.org/zap"
)

const (
	commentCacheOutboxBatchSize = 100
	commentCacheOutboxInterval  = 100 * time.Millisecond
)

func RunCommentCacheInvalidationOutbox(ctx context.Context, broker *eventmq.FavoriteEventBroker, svcCtx *svc.ServiceContext) {
	ticker := time.NewTicker(commentCacheOutboxInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if err := publishPendingCommentCacheInvalidations(ctx, broker, svcCtx); err != nil {
			zap.L().Error("发布评论缓存失效 Outbox 失败", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func publishPendingCommentCacheInvalidations(ctx context.Context, broker *eventmq.FavoriteEventBroker, svcCtx *svc.ServiceContext) error {
	if broker == nil || svcCtx == nil || svcCtx.DBList == nil || svcCtx.DBList.Mysql == nil {
		return errors.New("评论缓存失效 Outbox 依赖未初始化")
	}
	pending := make([]model.CommentCacheInvalidationOutbox, 0, commentCacheOutboxBatchSize)
	if err := svcCtx.DBList.Mysql.WithContext(ctx).
		Order("id ASC").Limit(commentCacheOutboxBatchSize).Find(&pending).Error; err != nil {
		return err
	}
	return processCommentCacheInvalidationOutboxBatch(pending, func(event eventmq.CacheInvalidationEvent) error {
		return broker.PublishCacheInvalidation(event)
	}, func(id uint64) error {
		return svcCtx.DBList.Mysql.WithContext(ctx).Delete(&model.CommentCacheInvalidationOutbox{}, id).Error
	})
}

func processCommentCacheInvalidationOutboxBatch(
	pending []model.CommentCacheInvalidationOutbox,
	publish func(eventmq.CacheInvalidationEvent) error,
	remove func(uint64) error,
) error {
	var firstErr error
	for _, item := range pending {
		event := eventmq.CacheInvalidationEvent{EventID: item.ID, VideoID: item.VideoID}
		if err := publish(event); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := remove(item.ID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
