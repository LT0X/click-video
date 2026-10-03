package model

import (
	"douyin/response"
	"douyin/rpc/video/video"
	"time"
)

// 这两个count是不是可以考虑删除 要的时候再去计算TODO
// 23.11.03 Title 增加全文索引 以便于搜索 ngram全文索引支持中文的插件 默认分词2
// 存文件名 然后灵活更换CDN域名
type Video struct {
	ID            uint64    `json:"id"`
	UploadID      string    `gorm:"type:varchar(36);uniqueIndex:uk_video_upload_id;not null" json:"upload_id"`
	AuthorID      uint64    `gorm:"not null;index" json:"author_id"`
	PlayURL       string    `gorm:"type:varchar(255);not null" json:"play_url"`
	CoverURL      string    `gorm:"type:varchar(255);not null" json:"cover_url"`
	Title         string    `gorm:"type:varchar(63);index:idx_title_topic,class:FULLTEXT,option:WITH PARSER ngram;not null" json:"title"`
	PublishTime   time.Time `gorm:"not null;index" json:"publish_time"`
	FavoriteCount int64     `gorm:"default:0;not null" json:"favorite_count"`
	CommentCount  int64     `gorm:"default:0;not null" json:"comment_count"`
	// 视频的分类 23.11.03新增 前两个为固定字段 后面为tag隐式搜索
	Topic string `gorm:"type:varchar(63);index:idx_title_topic,class:FULLTEXT,option:WITH PARSER ngram;not null" json:"topic"`
}

func TransformVideo(videoInfo *video.VideoInfo) *Video {
	return &Video{
		ID:            videoInfo.ID,
		AuthorID:      videoInfo.AuthorID,
		PlayURL:       videoInfo.PlayURL,
		CoverURL:      videoInfo.CoverURL,
		Title:         videoInfo.Title,
		PublishTime:   time.UnixMilli(videoInfo.PublishTime),
		FavoriteCount: videoInfo.FavoriteCount,
		CommentCount:  videoInfo.CommentCount,
		Topic:         videoInfo.Topic,
	}

}

func TransformVideoInfo(videoInfo *Video) *video.VideoInfo {
	return &video.VideoInfo{
		ID:            videoInfo.ID,
		AuthorID:      videoInfo.AuthorID,
		PlayURL:       videoInfo.PlayURL,
		CoverURL:      videoInfo.CoverURL,
		Title:         videoInfo.Title,
		PublishTime:   videoInfo.PublishTime.UnixMilli(),
		FavoriteCount: videoInfo.FavoriteCount,
		CommentCount:  videoInfo.CommentCount,
		Topic:         videoInfo.Topic,
	}

}

func TransformVideoInfos(videos []*Video) []*video.VideoInfo {
	res := make([]*video.VideoInfo, len(videos))
	for i, video := range videos {
		res[i] = TransformVideoInfo(video)
	}
	return res
}

func TransformVideoData(in []response.VideoData) []*video.VideoData {
	res := make([]*video.VideoData, len(in))
	for i, v := range in {
		user := v.User
		res[i] = &video.VideoData{}
		res[i].UserInfo = &video.UserInfo{
			ID:              user.ID,
			Username:        user.Name,
			Avatar:          user.Avatar,
			BackgroundImage: user.BackgroundImage,
			Signature:       user.Signature,
			FollowCount:     user.FollowCount,
			FollowerCount:   user.FollowerCount,
			TotalFavorited:  user.TotalFavorited,
			FavoriteCount:   user.FavoriteCount,
			WorkCount:       user.WorkCount,
		}
		res[i].VideoInfo = &video.VideoInfo{
			ID:            v.VideoID,
			AuthorID:      v.ID,
			PlayURL:       v.PlayURL,
			CoverURL:      v.CoverURL,
			Title:         v.Title,
			PublishTime:   v.PublishTime.UnixMilli(),
			FavoriteCount: v.FavoriteCount,
			CommentCount:  v.CommentCount,
			Topic:         v.Topic,
		}
	}
	return res
}
