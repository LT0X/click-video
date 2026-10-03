package logic

import (
	"context"
	model "douyin/rpc/contact/internal/modle"
	"fmt"
	"time"

	"douyin/rpc/contact/contact"
	"douyin/rpc/contact/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm/clause"
)

const maxChatMessageBatch = 50

type CreateMessagesBatchLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateMessagesBatchLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateMessagesBatchLogic {
	return &CreateMessagesBatchLogic{ctx: ctx, svcCtx: svcCtx, Logger: logx.WithContext(ctx)}
}

func (l *CreateMessagesBatchLogic) CreateMessagesBatch(in *contact.CreateMessagesBatchRequest) (*contact.CreateMessagesBatchResponse, error) {
	if in == nil || len(in.Messages) == 0 || len(in.Messages) > maxChatMessageBatch {
		return nil, fmt.Errorf("消息批次条数必须在 1 到 %d 之间", maxChatMessageBatch)
	}
	rows := make([]model.Message, 0, len(in.Messages))
	for _, item := range in.Messages {
		if item == nil || item.EventID == "" || len(item.EventID) > 64 || item.UserID == 0 || item.ToUserID == 0 || item.UserID == item.ToUserID || item.Content == "" || item.CreateTime <= 0 {
			return nil, fmt.Errorf("消息批次包含无效消息")
		}
		eventID := item.EventID
		rows = append(rows, model.Message{
			EventID: &eventID, Content: item.Content,
			CreateTime: time.UnixMilli(item.CreateTime), FromUserID: item.UserID, ToUserID: item.ToUserID,
		})
	}
	result := l.svcCtx.DBList.Mysql.WithContext(l.ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "event_id"}},
		DoNothing: true,
	}).CreateInBatches(&rows, maxChatMessageBatch)
	if result.Error != nil {
		return nil, fmt.Errorf("批量写入聊天消息失败: %w", result.Error)
	}
	return &contact.CreateMessagesBatchResponse{Reply: uint64(result.RowsAffected)}, nil
}
