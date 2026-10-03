package database

import (
	"douyin/model"
	"douyin/package/constant"
	"douyin/response"
	"time"
)

// service/publish.go 119
func SelectVideosByUserID(userID uint64) ([]model.Video, error) {
	videos := make([]model.Video, 0)
	err := constant.DB.Model(&model.Video{}).Where("author_id = ? ", userID).Order("publish_time desc").Find(&videos).Error
	if err != nil {
		return nil, err
	}
	return videos, nil
}

// 根据视频ID集合查询视频信息 批量查询 feed
func SelectVideoListByVideoID(videoIDList []uint64) ([]model.Video, error) {
	res := make([]model.Video, 0, len(videoIDList))
	// 这里按照id倒叙 其实id就能保证时间顺序了
	err := constant.DB.Where("id IN (?)", videoIDList).Order("id desc").Find(&res).Error
	if err != nil {
		return nil, err
	}
	return res, err
}

func UpdateVideoURL(playURL, coverURL string, videoID uint64) error {
	//  Don’t use Save with Model, it’s an Undefined Behavior.
	return constant.DB.Model(&model.Video{ID: videoID}).Updates(&model.Video{PlayURL: playURL, CoverURL: coverURL}).Error
}

// Scan支持的数据类型仅为struct及struct slice以及它们的指针类型
// Scan要不结构体加tag  gorm:"column:col_name" 指定列名 要不改造结构体
// service/video.go 30
func SelectFeedVideoList(numberVideos int, lastTime int64) ([]response.VideoData, error) {
	if lastTime == 0 {
		lastTime = time.Now().UnixMilli()
	}
	// TODO 下面的时间要用小于 可以考虑减1 用小于等于（为了使用索引？？）
	res := make([]response.VideoData, 0, 30)
	// 这里使用外连接 双表联查 可以考虑改多次单表 联查太麻烦-
	err := constant.DB.Model(&model.User{}).Select(`user.*,
    video.id as vid,
    video.play_url,
    video.cover_url,
    video.favorite_count as vfavorite_count,
    video.comment_count,
    video.title,
	video.publish_time,
	video.topic`).Joins(
		"right join video on video.author_id = user.id").Where("video.publish_time < ? ",
		time.UnixMilli(lastTime)).Order("video.publish_time desc").Limit(numberVideos).Scan(&res).Error
	if len(res) == 0 {
		newLastTime := time.Now().UnixMilli()
		err = constant.DB.Model(&model.User{}).Select(`user.*,
    	video.id as vid,
    	video.play_url,
		video.cover_url,
		video.favorite_count as vfavorite_count,
		video.comment_count,
		video.title,
		video.publish_time,
		video.topic`).Joins(
			"right join video on video.author_id = user.id").Where("video.publish_time < ? ",
			time.UnixMilli(newLastTime)).Order("video.publish_time desc").Limit(numberVideos).Scan(&res).Error
	}
	if err != nil {
		return nil, err
	}
	return res, nil
}

// //service/video.go 31
func SelectFeedVideoByTopic(numberVideos int, lastTime int64, topic string) ([]response.VideoData, error) {
	if lastTime == 0 {
		lastTime = time.Now().UnixMilli()
	}
	// TODO 下面的时间要用小于 可以考虑减1 用小于等于（为了使用索引？？）
	res := make([]response.VideoData, 0, 30)
	// 这里使用外连接 双表联查 可以考虑改多次单表 联查太麻烦
	err := constant.DB.Model(&model.User{}).Select(`user.*,
    video.id as vid,
    video.play_url,
    video.cover_url,
    video.favorite_count as vfavorite_count,
    video.comment_count,
    video.title,
	video.publish_time,
	video.topic`).Joins(
		"right join video on video.author_id = user.id").Where("video.publish_time < ? and video.topic like ?",
		time.UnixMilli(lastTime), topic+"%").Order("video.publish_time desc").Limit(numberVideos).Scan(&res).Error
	if len(res) == 0 {
		newLastTime := time.Now().UnixMilli()
		err = constant.DB.Model(&model.User{}).Select(`user.*,
		video.id as vid,
		video.play_url,
		video.cover_url,
		video.favorite_count as vfavorite_count,
		video.comment_count,
		video.title,
		video.publish_time,
		video.topic`).Joins(
			"right join video on video.author_id = user.id").Where("video.publish_time < ? and video.topic like ?",
			time.UnixMilli(newLastTime), topic+"%").Order("video.publish_time desc").Limit(numberVideos).Scan(&res).Error
	}

	if err != nil {
		return nil, err
	}
	return res, nil
}

// func SelectPublishTimeByVideoID(vid uint64) (time.Time, error) {
// 	var publishTime time.Time
// 	err := constant.DB.Model(&model.Video{}).Select("publish_time").Where("id = ?", vid).First(&publishTime).Error
// 	return publishTime, err
// }

func SearchVideoByKeyword(keyword string) ([]model.Video, error) {
	var videos []model.Video
	err := constant.DB.Raw("select * from video where match(title,topic) against(?) order by publish_time desc", keyword).Scan(&videos).Error
	return videos, err
}
