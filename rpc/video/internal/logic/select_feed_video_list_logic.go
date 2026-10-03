package logic

import (
	"context"

	"douyin/response"
	"douyin/rpc/video/internal/cache"
	"douyin/rpc/video/internal/model"
	"time"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
)

type SelectFeedVideoListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewSelectFeedVideoListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SelectFeedVideoListLogic {
	return &SelectFeedVideoListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *SelectFeedVideoListLogic) SelectFeedVideoList(in *video.SelectFeedVideoListRequest) (*video.SelectFeedVideoListResponse, error) {
	// todo: add your logic here and delete this line
	if in.LastTime == 0 {
		in.LastTime = time.Now().UnixMilli()
	}
	// TODO 下面的时间要用小于 可以考虑减1 用小于等于（为了使用索引？？）
	res := make([]response.VideoData, 0, 30)
	// 这里使用外连接 双表联查 可以考虑改多次单表 联查太麻烦-
	err := l.svcCtx.DBList.Mysql.Model(&model.User{}).Select(`user.*,
    video.id as vid,
    video.play_url,
    video.cover_url,
    video.favorite_count as vfavorite_count,
    video.comment_count,
    video.title,
	video.publish_time,
	video.topic`).Joins(
		"right join video on video.author_id = user.id").Where("video.publish_time < ? ",
		time.UnixMilli(in.LastTime)).Order("video.publish_time desc").Limit(int(in.NumberVideos)).Scan(&res).Error
	if len(res) == 0 {
		newLastTime := time.Now().UnixMilli()
		err = l.svcCtx.DBList.Mysql.Model(&model.User{}).Select(`user.*,
    	video.id as vid,
    	video.play_url,
		video.cover_url,
		video.favorite_count as vfavorite_count,
		video.comment_count,
		video.title,
		video.publish_time,
		video.topic`).Joins(
			"right join video on video.author_id = user.id").Where("video.publish_time < ? ",
			time.UnixMilli(newLastTime)).Order("video.publish_time desc").Limit(int(in.NumberVideos)).Scan(&res).Error
	}
	if err != nil {
		return nil, err
	}
	favoriteCountRows := make([]*model.Video, len(res))
	for i, item := range res {
		favoriteCountRows[i] = &model.Video{ID: item.VideoID, FavoriteCount: item.VideoFavoriteCount}
	}
	if err := cache.LoadFavoriteCounts(l.ctx, favoriteCountRows); err != nil {
		return nil, err
	}
	for i := range res {
		res[i].VideoFavoriteCount = favoriteCountRows[i].FavoriteCount
	}
	return &video.SelectFeedVideoListResponse{
		VideoDatas: model.TransformVideoData(res),
	}, nil
}
