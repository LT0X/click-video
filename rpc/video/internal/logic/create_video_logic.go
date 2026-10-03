package logic

import (
	"context"
	"errors"
	"strings"

	"douyin/rpc/video/internal/model"
	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/gofrs/uuid"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type CreateVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateVideoLogic {
	return &CreateVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateVideoLogic) CreateVideo(in *video.CreateVideoRequest) (*video.CreateVideoResponse, error) {
	if in == nil || in.VideoID == nil || in.VideoID.AuthorID == 0 ||
		strings.TrimSpace(in.VideoID.Title) == "" || strings.TrimSpace(in.VideoID.PlayURL) == "" {
		return nil, errors.New("视频作者、标题和播放地址不能为空")
	}
	parsedUploadID, err := uuid.FromString(in.UploadID)
	if err != nil {
		return nil, errors.New("上传 ID 必须为 UUID")
	}
	videoRecord := model.TransformVideo(in.VideoID)
	videoRecord.UploadID = parsedUploadID.String()
	videoRecord.ID = 0 // 视频 ID 只能由 video.rpc 数据库生成。
	err = l.svcCtx.DBList.Mysql.WithContext(l.ctx).Transaction(func(tx *gorm.DB) error {
		created := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "upload_id"}},
			DoNothing: true,
		}).Create(videoRecord)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 1 {
			return nil
		}

		var existing model.Video
		if err := tx.Where("upload_id = ?", videoRecord.UploadID).First(&existing).Error; err != nil {
			return err
		}
		if existing.AuthorID != videoRecord.AuthorID || existing.Title != videoRecord.Title || existing.PlayURL != videoRecord.PlayURL {
			return errors.New("上传 ID 已关联到不同的视频信息")
		}
		videoRecord.ID = existing.ID
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &video.CreateVideoResponse{VideoID: videoRecord.ID}, nil
}
