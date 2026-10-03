package logic

import (
	"context"
	"douyin/rpc/video/internal/model"
	"gorm.io/gorm"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
)

type CommentDeleteLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCommentDeleteLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CommentDeleteLogic {
	return &CommentDeleteLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CommentDeleteLogic) CommentDelete(in *video.CommentDeleteRequest) (*video.CommentDeleteResponse, error) {
	// todo: add your logic here and delete this line
	comment := model.Comment{}
	// delete 不会回写到comment里  Clauses(clause.Returning{}) 这个才会回写

	err := l.svcCtx.DBList.Mysql.Transaction(func(tx *gorm.DB) error {
		// 删除要先检查里面有没有啊
		err := tx.Where("id = ? AND video_id = ? AND user_id = ?", in.CommentID, in.VideoID, in.UserID).First(&comment).Error
		if err != nil {
			return err
		}
		if comment.ID == 0 {
			return gorm.ErrRecordNotFound
		}
		err = tx.Delete(&comment).Error
		if err != nil {
			return err
		}
		video := model.Video{ID: in.VideoID}
		err = tx.Model(&video).Select("comment_count").First(&video).Error
		if err != nil {
			return err
		}
		err = tx.Model(&video).Update("comment_count", video.CommentCount-1).Error
		if err != nil {
			return err
		}
		return tx.Create(&model.CommentCacheInvalidationOutbox{VideoID: in.VideoID}).Error
	})
	if err != nil {
		return nil, err
	}

	res := model.TransformRPCComment(&comment)

	return &video.CommentDeleteResponse{
		Comment: res,
	}, nil
}
