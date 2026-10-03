package model

import (
	"douyin/rpc/video/video"
	"time"
)

// 这两个count是不是可以考虑删除 要的时候再去计算TODO
// 23.11.03 Title 增加全文索引 以便于搜索 ngram全文索引支持中文的插件 默认分词2
// 存文件名 然后灵活更换CDN域名
type Video struct {
	ID            uint64    `json:"id"`
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

func TransformVideoInfo(info *video.VideoInfo) Video {
	return Video{
		ID:            info.ID,
		AuthorID:      info.AuthorID,
		PlayURL:       info.PlayURL,
		CoverURL:      info.CoverURL,
		Title:         info.Title,
		PublishTime:   time.UnixMilli(info.PublishTime),
		FavoriteCount: info.FavoriteCount,
		CommentCount:  info.CommentCount,
		Topic:         info.Topic,
	}
}

func TransformVideoInfos(infos []*video.VideoInfo) []Video {
	result := make([]Video, len(infos))
	for i, info := range infos {
		result[i] = TransformVideoInfo(info)
	}
	return result
}

//func TransformVideoData(in []*video.VideoData) []response.VideoData {
//	res := make([]response.VideoData, len(in))
//	for i, v := range in {
//		user := v.UserInfo
//		res[i].User = response.User{
//			ID:              user.ID,
//			Name:            user.Username,
//			Avatar:          user.Avatar,
//			BackgroundImage: user.BackgroundImage,
//			Signature:       user.Signature,
//			FollowCount:     user.FollowCount,
//			FollowerCount:   user.FollowerCount,
//			TotalFavorited:  user.TotalFavorited,
//			FavoriteCount:   user.FavoriteCount,
//			WorkCount:       user.WorkCount,
//		}
//		res[i].VideoID = v.VideoInfo.ID
//		res[i].PlayURL = v.VideoInfo.PlayURL
//		res[i].CoverURL = v.VideoInfo.CoverURL
//		res[i].VideoFavoriteCount = v.VideoInfo.FavoriteCount
//		res[i].CommentCount = v.VideoInfo.CommentCount
//		res[i].Title = v.VideoInfo.Title
//		res[i].Topic = v.VideoInfo.Topic
//		res[i].PublishTime = time.UnixMilli(v.VideoInfo.PublishTime)
//		res[i].FavoriteCount = v.VideoInfo.FavoriteCount
//
//	}
//	return res
//}
