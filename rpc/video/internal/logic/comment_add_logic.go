package logic

import (
	"context"

	"douyin/rpc/video/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CommentAddLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCommentAddLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CommentAddLogic {
	return &CommentAddLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CommentAddLogic) CommentAdd(in *video.CommentAddRequest) (*video.CommentAddResponse, error) {
	// todo: add your logic here and delete this line
	if in == nil || in.Comment == nil {
		return nil, status.Error(codes.InvalidArgument, "评论信息不能为空")
	}
	exists, err := videoExists(l.ctx, l.svcCtx, in.Comment.VideoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, status.Error(codes.NotFound, "视频不存在")
	}

	err = l.svcCtx.DBList.Mysql.Transaction(func(tx *gorm.DB) error {
		// 先查询videoID是否存在
		com := model.TransformComment(in.Comment)
		video := model.Video{ID: com.VideoID}

		err := tx.First(&video).Error
		if err != nil {
			return err
		}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&com)
		err = result.Error
		if err != nil {
			return err
		}
		// RabbitMQ 至少一次投递；同一 ID 已写入时不重复增加评论计数。
		if result.RowsAffected == 0 {
			return nil
		}
		err = tx.Model(&video).UpdateColumn("comment_count", gorm.Expr("comment_count + ?", 1)).Error
		if err != nil {
			return err
		}
		return tx.Create(&model.CommentCacheInvalidationOutbox{VideoID: com.VideoID}).Error
	})
	if err != nil {
		return nil, err
	}

	return &video.CommentAddResponse{
		Reply: 1,
	}, nil
}
