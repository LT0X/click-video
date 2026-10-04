package mq

import (
	"context"
	"errors"

	eventmq "douyin/package/mq"
	"douyin/rpc/user/internal/cache"
	"douyin/rpc/user/internal/logic"
	"douyin/rpc/user/internal/svc"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func RunFavoriteActionConsumer(ctx context.Context, broker *eventmq.FavoriteEventBroker, svcCtx *svc.ServiceContext) {
	broker.RunFavoriteActions(ctx, func(ctx context.Context, event eventmq.FavoriteActionEvent) error {
		_, favoriteCount, err := logic.ApplyFavoriteAction(ctx, svcCtx, event.EventID, event.ActionSequence, event.UserID, event.AuthorID, event.VideoID, event.Cnt)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return eventmq.DiscardFavoriteMessage(err)
			}
			return err
		}
		if err := cache.SetFavoriteActionResult(event.EventID, favoriteCount); err != nil {
			// 结果头是可选展示信息，Redis 故障不能让已提交的业务动作反复重投。
			zap.L().Warn("写入点赞动作计数结果失败", zap.Error(err))
		}
		return nil
	})
}
