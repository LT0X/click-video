package mq

import (
	"context"

	eventmq "douyin/package/mq"
	"douyin/rpc/video/internal/logic"
	"douyin/rpc/video/internal/svc"
)

func RunFavoriteCountConsumer(ctx context.Context, broker *eventmq.FavoriteEventBroker, svcCtx *svc.ServiceContext) {
	broker.RunFavoriteCounts(ctx, func(_ context.Context, event eventmq.FavoriteCountEvent) error {
		return logic.ApplyFavoriteCountEvent(event)
	})
}
