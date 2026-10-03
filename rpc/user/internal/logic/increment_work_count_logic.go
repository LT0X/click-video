package logic

import (
	"context"
	"errors"

	"douyin/rpc/user/internal/model"
	"douyin/rpc/user/internal/svc"
	"douyin/rpc/user/user"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IncrementWorkCountLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewIncrementWorkCountLogic(ctx context.Context, svcCtx *svc.ServiceContext) *IncrementWorkCountLogic {
	return &IncrementWorkCountLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *IncrementWorkCountLogic) IncrementWorkCount(in *user.IncrementWorkCountRequest) (*user.IncrementWorkCountResponse, error) {
	if in == nil || in.UserID == 0 || in.VideoID == 0 {
		return nil, errors.New("用户 ID 和视频 ID 必须大于 0")
	}
	applied := false
	err := l.svcCtx.DBList.Mysql.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		ledger := model.WorkCountVideo{VideoID: in.VideoID, UserID: in.UserID}
		created := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "video_id"}},
			DoNothing: true,
		}).Create(&ledger)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			var existing model.WorkCountVideo
			if err := tx.First(&existing, "video_id = ?", in.VideoID).Error; err != nil {
				return err
			}
			if existing.UserID != in.UserID {
				return errors.New("视频已归属其他作者，拒绝重复计数")
			}
			return nil
		}

		updated := tx.Model(&model.User{}).Where("id = ?", in.UserID).
			UpdateColumn("work_count", gorm.Expr("work_count + ?", 1))
		if updated.Error != nil {
			return updated.Error
		}
		if updated.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
		applied = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &user.IncrementWorkCountResponse{Applied: applied}, nil
}
