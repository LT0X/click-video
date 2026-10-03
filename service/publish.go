package service

import (
	"bytes"
	"context"
	"douyin/database"
	"douyin/model"
	"douyin/package/cache"
	"douyin/package/constant"
	"douyin/package/util"
	"douyin/response"
	"douyin/rpc/user/user"
	"douyin/rpc/video/video"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
)

type PublisService struct {
	// 用户鉴权token
	Token string `form:"token"`
	// 视频标题
	Title string `form:"title"`
	// 新增 topic
	Topic string `form:"topic"`
}

type PublishListService struct {
	// 用户鉴权token
	Token string `query:"token"`
	// 用户id
	UserID uint64 `query:"user_id"`
}

func (service *PublisService) PublishAction(userID uint64, buf *bytes.Buffer) (*response.CommonResponse, error) {
	// 生成唯一文件名
	u1, err := uuid.NewV4()
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	fileName := u1.String() + "." + "mp4"
	playURL, coverURL, err := util.UploadVideo(buf.Bytes(), fileName)
	playURL = "http://127.0.0.1:8000/static/playurl/" + playURL

	if err != nil {
		return nil, err
	}
	switch service.Topic {
	case constant.TopicSport:
	case constant.TopicGame:
	case constant.TopicMusic:
	default:
		service.Topic = constant.TopicDefualt + service.Topic
	}
	created, err := database.RPC.VideoRpc.CreateVideo(context.Background(), &video.CreateVideoRequest{
		UploadID: u1.String(),
		VideoID: &video.VideoInfo{
			AuthorID:    userID,
			PlayURL:     playURL,
			CoverURL:    coverURL,
			Title:       service.Title,
			Topic:       service.Topic,
			PublishTime: time.Now().UnixMilli(),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("通过 video.rpc 创建视频失败: %w", err)
	}
	if created == nil || created.VideoID == 0 {
		return nil, errors.New("video.rpc 返回了无效的视频 ID")
	}
	if cache.VideoIDBloomFilter != nil {
		cache.VideoIDBloomFilter.AddString(strconv.FormatUint(created.VideoID, 10))
	}
	if cache.UserRedisClient != nil && cache.VideoRedisClient != nil {
		if err := cache.PublishVideo(userID, created.VideoID); err != nil {
			zap.L().Warn("清除发布列表缓存失败", zap.Uint64("video_id", created.VideoID), zap.Error(err))
		}
	}
	_, err = database.RPC.UserRpc.IncrementWorkCount(context.Background(), &user.IncrementWorkCountRequest{
		UserID:  userID,
		VideoID: created.VideoID,
	})
	if err != nil {
		return nil, fmt.Errorf("通过 user.rpc 更新作品数失败: %w", err)
	}
	return &response.CommonResponse{
		StatusCode: response.Success,
		StatusMsg:  response.UploadVideoSuccess,
	}, nil
}

func (service *PublishListService) GetPublishVideos(loginUserID uint64) (*response.VideoListResponse, error) {
	// 第一步查找 所有的 service.user_id 的视频记录
	// 然后 对这些视频判断 loginUserID 有没有点赞
	// 视频里的作者信息应当都是service.user_id（还需判断 登录用户有没有关注）
	// TODO 加分布式锁 redis
	// TODO 这里其实应当先去redis拿列表 再去数据库拿数据

	videoResp, err := database.RPC.VideoRpc.SelectVideosByUserID(context.TODO(), &video.SelectVideosByUserIDRequest{
		UserID: service.UserID,
	})
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	videos := model.TransformVideoInfos(videoResp.Videos)
	// 不都是一个作者嘛 拿一次信息不就好了
	author, err := cache.GetUserInfo(service.UserID)
	if err != nil {
		zap.L().Warn(constant.CacheMiss, zap.Error(err))
		author, err = database.SelectUserByID(service.UserID)
		if err != nil {
			zap.L().Error(err.Error())
			return nil, err
		}
		go func() {
			err := cache.SetUserInfo(author)
			if err != nil {
				zap.L().Error(err.Error())
			}
		}()
	}
	var isFollowed bool
	if loginUserID == 0 {
		isFollowed = false
	}
	if service.UserID == loginUserID {
		isFollowed = true
	} else {
		isFollowed, err = cache.IsFollow(loginUserID, service.UserID)
		if err != nil {
			zap.L().Warn(constant.CacheMiss)
			isFollowed, err = database.IsFollowed(loginUserID, service.UserID)
			if err != nil {
				zap.L().Error(err.Error())
				return nil, err
			}
			go func() {
				following, err := database.SelectFollowingByUserID(loginUserID)
				if err != nil {
					zap.L().Error(err.Error())
					return
				}
				err = cache.SetFollowUserIDSet(loginUserID, following)
				if err != nil {
					zap.L().Error(err.Error())
				}
			}()
		}
	}
	var favorite []uint64
	if loginUserID != 0 {
		favorite, err = cache.GetFavoriteSet(loginUserID)
		if err != nil {
			zap.L().Warn(constant.CacheMiss)

			//favorite, err = database.SelectFavoriteVideoByUserID(loginUserID)
			resp, err := database.RPC.UserRpc.SelectFavoriteVideoByUserID(context.TODO(), &user.SelectFavoriteVideoByUserIDRequest{
				UserID: loginUserID,
			})
			favorite = resp.UserIDs

			if err != nil {
				zap.L().Error(err.Error())
				return nil, err
			}
			go func() {
				err := cache.SetFavoriteSet(loginUserID, favorite)
				if err != nil {
					zap.L().Error(err.Error())
				}
			}()
		}
	}
	favoriteMap := make(map[uint64]struct{}, len(favorite))
	for _, ff := range favorite {
		favoriteMap[ff] = struct{}{}
	}
	// 构造返回参数
	reps := make([]response.Video, 0, len(videos))
	for i, ff := range videos {
		item := response.Video{
			ID:            videos[i].ID,
			CommentCount:  videos[i].CommentCount,
			CoverURL:      videos[i].CoverURL,
			FavoriteCount: videos[i].FavoriteCount,
			PlayURL:       videos[i].PlayURL,
			Title:         videos[i].Title,
			Author:        *response.UserInfo(author, isFollowed),
			PublishTime:   videos[i].PublishTime.Format("2006-01-02 15:04"),
			Topic:         videos[i].Topic,
		}
		if _, ok := favoriteMap[ff.ID]; ok {
			item.IsFavorite = true
		}
		reps = append(reps, item)
	}
	return &response.VideoListResponse{
		StatusCode: response.Success,
		StatusMsg:  response.PubulishListSuccess,
		VideoList:  reps,
	}, nil
}
