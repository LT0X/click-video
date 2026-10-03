package logic

import (
	"context"
	"douyin/rpc/video/internal/cache"
	"douyin/rpc/video/internal/model"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
)

type SearchVideoByKeywordLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSearchVideoByKeywordLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SearchVideoByKeywordLogic {
	return &SearchVideoByKeywordLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SearchVideoByKeywordLogic) SearchVideoByKeyword(in *video.SearchVideoByKeywordRequest) (*video.SearchVideoByKeywordResponse, error) {
	// todo: add your logic here and delete this line
	var videos []*model.Video
	err := l.svcCtx.DBList.Mysql.Raw("select * from video where match(title,topic) against(?) order by publish_time desc", in.Keyword).Scan(&videos).Error
	if err != nil {
		return nil, err
	}
	if err := cache.LoadFavoriteCounts(l.ctx, videos); err != nil {
		return nil, err
	}
	return &video.SearchVideoByKeywordResponse{
		VideInfos: model.TransformVideoInfos(videos),
	}, nil
}
