package mq

import (
	"context"
	"errors"

	eventmq "douyin/package/mq"
	"douyin/rpc/user/internal/logic"
	"douyin/rpc/user/internal/svc"
	"gorm.io/gorm"
)

func RunFavoriteActionConsumer(ctx context.Context, broker *eventmq.FavoriteEventBroker, svcCtx *svc.ServiceContext) {
	broker.RunFavoriteActions(ctx, func(ctx context.Context, event eventmq.FavoriteActionEvent) error {
		_, err := logic.ApplyFavoriteAction(ctx, svcCtx, event.EventID, event.ActionSequence, event.UserID, event.AuthorID, event.VideoID, event.Cnt)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return eventmq.DiscardFavoriteMessage(err)
			}
			return err
		}
		return nil
	})
}
