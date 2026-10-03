package logic

import (
	"context"
	"douyin/rpc/video/internal/cache"
	"douyin/rpc/video/internal/model"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
)

type SelectVideosByUserIDLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSelectVideosByUserIDLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SelectVideosByUserIDLogic {
	return &SelectVideosByUserIDLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SelectVideosByUserIDLogic) SelectVideosByUserID(in *video.SelectVideosByUserIDRequest) (*video.SelectVideosByUserIDResponse, error) {
	// todo: add your logic here and delete this line

	videos := make([]*model.Video, 0)
	err := l.svcCtx.DBList.Mysql.Model(&model.Video{}).Where("author_id = ? ", in.UserID).Order("publish_time desc").Find(&videos).Error
	if err != nil {
		return nil, err
	}
	if err := cache.LoadFavoriteCounts(l.ctx, videos); err != nil {
		return nil, err
	}

	return &video.SelectVideosByUserIDResponse{
		Videos: model.TransformVideoInfos(videos),
	}, nil
}
