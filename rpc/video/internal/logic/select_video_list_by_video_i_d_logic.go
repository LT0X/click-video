package logic

import (
	"context"
	"douyin/rpc/video/internal/cache"
	"douyin/rpc/video/internal/model"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
)

type SelectVideoListByVideoIDLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSelectVideoListByVideoIDLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SelectVideoListByVideoIDLogic {
	return &SelectVideoListByVideoIDLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SelectVideoListByVideoIDLogic) SelectVideoListByVideoID(in *video.SelectVideoListByVideoIDRequest) (*video.SelectVideoListByVideoIDResponse, error) {
	// todo: add your logic here and delete this line
	res := make([]*model.Video, 0, len(in.VideoIDList))
	// 这里按照id倒叙 其实id就能保证时间顺序了
	err := l.svcCtx.DBList.Mysql.Where("id IN (?)", in.VideoIDList).Order("id desc").Find(&res).Error
	if err != nil {
		return nil, err
	}
	if err := cache.LoadFavoriteCounts(l.ctx, res); err != nil {
		return nil, err
	}
	return &video.SelectVideoListByVideoIDResponse{
		Videos: model.TransformVideoInfos(res),
	}, nil
}
