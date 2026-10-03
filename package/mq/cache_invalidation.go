package mq

import (
	"context"

	"douyin/package/cache"
)

func runCacheInvalidationConsumer(ctx context.Context, broker *FavoriteEventBroker) {
	if broker == nil {
		return
	}
	broker.RunCacheInvalidations(ctx, func(ctx context.Context, event CacheInvalidationEvent) error {
		return ProcessCacheInvalidationEvent(event, cache.InvalidateCommentCache, broker.PublishCacheInvalidation)
	})
}

// ProcessCacheInvalidationEvent 先删除当前缓存，再将第二次删除交给 TTL+DLX 队列延迟执行。
func ProcessCacheInvalidationEvent(event CacheInvalidationEvent, invalidate func(uint64) error, publish func(CacheInvalidationEvent) error) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if err := invalidate(event.VideoID); err != nil {
		return err
	}
	if event.Phase == CacheInvalidationImmediate {
		event.Phase = CacheInvalidationDelayed
		return publish(event)
	}
	return nil
}
