package service

import (
	"context"
	"douyin/database"
	"douyin/model"
	"douyin/package/cache"
	"douyin/package/constant"
	"douyin/package/metrics"
	"douyin/package/mq"
	"douyin/package/util"
	"douyin/response"
	"douyin/rpc/user/user"
	"douyin/rpc/video/video"
	"errors"
	"github.com/zeromicro/go-zero/core/mr"
	"time"

	"go.uber.org/zap"
)

type FavoriteService struct {
	// 1-点赞，2-取消点赞
	ActionType string `query:"action_type"`
	// 用户鉴权token
	Token string `query:"token"`
	// 视频id
	VideoID uint64 `query:"video_id"`
	// 要查询的用户id
	UserID uint64 `query:"user_id"`
}

func (service *FavoriteService) Favorite(userID uint64) (*response.CommonResponse, *int64, error) {
	// TODO 可以拿redis限制一下用户点赞的速率 比如1分钟只能点赞10次
	startedAt := time.Now()
	eventID, err := service.sendFavoriteAction(userID, 1)
	if err != nil {
		recordFavoriteRequestMetrics(metrics.Default, "favorite", err, time.Since(startedAt))
		zap.L().Error(err.Error())
		return nil, nil, err
	}
	var favoriteCount *int64
	if count, ok := cache.WaitFavoriteActionCount(eventID); ok {
		favoriteCount = &count
	}
	recordFavoriteRequestMetrics(metrics.Default, "favorite", nil, time.Since(startedAt))
	return &response.CommonResponse{
		StatusCode: response.Success,
		StatusMsg:  constant.FavoriteSuccess,
	}, favoriteCount, nil
}

func (service *FavoriteService) UnFavorite(userID uint64) (*response.CommonResponse, *int64, error) {
	startedAt := time.Now()
	eventID, err := service.sendFavoriteAction(userID, -1)
	if err != nil {
		recordFavoriteRequestMetrics(metrics.Default, "unfavorite", err, time.Since(startedAt))
		zap.L().Error(err.Error())
		return nil, nil, err
	}
	var favoriteCount *int64
	if count, ok := cache.WaitFavoriteActionCount(eventID); ok {
		favoriteCount = &count
	}
	recordFavoriteRequestMetrics(metrics.Default, "unfavorite", nil, time.Since(startedAt))
	return &response.CommonResponse{
		StatusCode: response.Success,
		StatusMsg:  constant.UnFavoriteSuccess,
	}, favoriteCount, nil
}

func recordFavoriteRequestMetrics(registry *metrics.Registry, action string, requestErr error, duration time.Duration) {
	result := "success"
	if requestErr != nil {
		result = "error"
	}
	registry.ObserveFavoriteRequest(action, result, duration)
}

func (service *FavoriteService) sendFavoriteAction(userID uint64, delta int64) (uint64, error) {
	eventID, err := util.GetSonyFlakeID()
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sequenceResp, err := database.RPC.UserRpc.ReserveFavoriteActionSequence(ctx, &user.ReserveFavoriteActionSequenceRequest{
		UserID:  userID,
		VideoID: service.VideoID,
	})
	if err != nil {
		return 0, err
	}
	videoInfo, err := database.RPC.VideoRpc.SelectVideoListByVideoID(ctx, &video.SelectVideoListByVideoIDRequest{
		VideoIDList: []uint64{service.VideoID},
	})
	if err != nil {
		return 0, err
	}
	if len(videoInfo.Videos) == 0 || videoInfo.Videos[0].AuthorID == 0 {
		return 0, errors.New(constant.BadParaRequest)
	}
	if err := mq.SendFavoriteMessage(mq.FavoriteActionEvent{
		EventID:        eventID,
		ActionSequence: sequenceResp.Sequence,
		UserID:         userID,
		AuthorID:       videoInfo.Videos[0].AuthorID,
		VideoID:        service.VideoID,
		Cnt:            delta,
	}); err != nil {
		return 0, err
	}
	return eventID, nil
}

func (service *FavoriteService) FavoriteList(userID uint64) ([]response.Video, error) {
	// TODO 加分布式锁
	// redis查找所有喜欢的视频ID
	videoIDs, err := cache.GetFavoriteSet(service.UserID)
	if err != nil {
		zap.L().Warn(constant.CacheMiss)

		//videoIDs, err = database.SelectFavoriteVideoByUserID(service.UserID)
		resp, err := database.RPC.UserRpc.SelectFavoriteVideoByUserID(context.TODO(), &user.SelectFavoriteVideoByUserIDRequest{
			UserID: service.UserID,
		})
		videoIDs = resp.UserIDs

		if err != nil {
			zap.L().Error(err.Error())
			return nil, err
		}
		// 加入到缓存里
		go func() {
			err := cache.SetFavoriteSet(service.UserID, videoIDs)
			if err != nil {
				zap.L().Error(err.Error())
			}
		}()
	}
	// 然后去数据库批量查找视频数据
	// TODO 要去redis查找视频信息 否则存视频没有意义
	// 但是我又觉得最终还是要走DB 一开始走不就行
	// 再看看怎么写合理 暂时只走数据库（db肯定是顺序的）
	// 最重点的redis返回的videoIDs 不是顺序的
	// 那么走redis查到的数据是乱序的（用zset解决 但是代码复杂）

	//确定拿mapReduce 优化
	videoResp, err := database.RPC.VideoRpc.SelectVideoListByVideoID(context.TODO(), &video.SelectVideoListByVideoIDRequest{
		VideoIDList: videoIDs,
	})
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	videos := model.TransformVideoInfos(videoResp.Videos)

	// 拿到视频数据之后 还得一个视频一个视频拿到作者信息
	userIDs := make([]uint64, 0, len(videoIDs))
	for _, video := range videos {
		userIDs = append(userIDs, video.AuthorID)
	}

	// 判断用户有没有关注 获取用户关注列表
	var following []uint64
	if userID != 0 {
		following, err = cache.GetFollowUserIDSet(userID)
		if err != nil {
			zap.L().Warn(constant.CacheMiss)
			following, err = database.SelectFollowingByUserID(userID)
			if err != nil {
				zap.L().Error(err.Error())
				return nil, err
			}
			go func() {
				err := cache.SetFollowUserIDSet(userID, following)
				if err != nil {
					zap.L().Error(err.Error())
				}
			}()
		}
	}
	followingMap := make(map[uint64]struct{}, len(following))
	for _, i := range following {
		followingMap[i] = struct{}{}
	}
	// 把自己放进去
	followingMap[userID] = struct{}{}

	videoResponse := make([]response.Video, len(videos), len(videos))

	orderMp := make(map[uint64]int, len(videos))
	// 批量拿到作者信息 但是还需要填空 哪个作者对应哪个

	mr.MapReduce(func(source chan<- interface{}) {
		for i, v := range videos {
			source <- i
			orderMp[v.ID] = i
		}
	}, func(item interface{}, writer mr.Writer, cancel func(error)) {
		i := item.(int)

		resp, err := database.RPC.UserRpc.SelectUserByID(context.TODO(), &user.SelectUserByIDRequest{
			UserID: videos[i].AuthorID,
		})
		if err != nil {
			zap.L().Error(err.Error())
			return
		}
		userInfo := model.TransformUser(resp.User)
		_, isFollowing := followingMap[userInfo.ID]
		user1 := *response.UserInfo(userInfo, isFollowing)

		video := response.Video{
			Author:        user1,
			CommentCount:  videos[i].CommentCount,
			CoverURL:      videos[i].CoverURL,
			FavoriteCount: videos[i].FavoriteCount,
			ID:            videos[i].ID,
			IsFavorite:    true,
			PlayURL:       videos[i].PlayURL,
			Title:         videos[i].Title,
			PublishTime:   videos[i].PublishTime.Format("2006-01-02 15:04"),
			Topic:         videos[i].Topic,
		}

		writer.Write(video)

	}, func(pipe <-chan interface{}, writer mr.Writer, cancel func(error)) {
		for item := range pipe {
			vv := item.(response.Video)
			videoResponse[orderMp[vv.ID]] = vv
		}
	})

	//zap.L().Warn("应该是完成了")

	//usersData, err := database.SelectUserListByIDs(userIDs)
	//if err != nil {
	//	zap.L().Error(err.Error())
	//	return nil, err
	//}

	//// 判断用户有没有关注 获取用户关注列表
	//var following []uint64
	//if userID != 0 {
	//	following, err = cache.GetFollowUserIDSet(userID)
	//	if err != nil {
	//		zap.L().Warn(constant.CacheMiss)
	//		following, err = database.SelectFollowingByUserID(userID)
	//		if err != nil {
	//			zap.L().Error(err.Error())
	//			return nil, err
	//		}
	//		go func() {
	//			err := cache.SetFollowUserIDSet(userID, following)
	//			if err != nil {
	//				zap.L().Error(err.Error())
	//			}
	//		}()
	//	}
	//}
	//followingMap := make(map[uint64]struct{}, len(following))
	//for _, i := range following {
	//	followingMap[i] = struct{}{}
	//}

	//// 把自己放进去
	//followingMap[userID] = struct{}{}
	//usersMap := make(map[uint64]*model.User, len(usersData))
	//for i, id := range usersData {
	//	usersMap[id.ID] = &usersData[i]
	//}
	//var favorite []uint64
	//if userID != 0 {
	//	// 获取登录用户点赞列表
	//	favorite, err = cache.GetFavoriteSet(userID)
	//	if err != nil {
	//		zap.L().Warn(constant.CacheMiss)
	//		//favorite, err = database.SelectFavoriteVideoByUserID(userID)
	//
	//		resp, err := database.RPC.UserRpc.SelectFavoriteVideoByUserID(context.TODO(), &user.SelectFavoriteVideoByUserIDRequest{
	//			UserID: userID,
	//		})
	//		favorite = resp.UserIDs
	//
	//		if err != nil {
	//			zap.L().Error(err.Error())
	//			return nil, err
	//		}
	//		go func() {
	//			err := cache.SetFavoriteSet(userID, favorite)
	//			if err != nil {
	//				zap.L().Error(err.Error())
	//			}
	//		}()
	//	}
	//}
	//favoriteMap := make(map[uint64]struct{}, len(favorite))
	//for _, i := range favorite {
	//	favoriteMap[i] = struct{}{}
	//}
	//videoResponse := make([]response.Video, 0, len(videos))
	//for _, video := range videos {
	//	if _, ok := usersMap[video.AuthorID]; ok {
	//		_, isFollowing := followingMap[video.AuthorID]
	//		vv := response.Video{
	//			Author:        *response.UserInfo(usersMap[video.AuthorID], isFollowing),
	//			CommentCount:  video.CommentCount,
	//			CoverURL:      config.System.Qiniu.OssDomain + "/" + video.CoverURL,
	//			FavoriteCount: video.FavoriteCount,
	//			ID:            video.ID,
	//			IsFavorite:    false,
	//			PlayURL:       config.System.Qiniu.OssDomain + "/" + video.PlayURL,
	//			Title:         video.Title,
	//			PublishTime:   video.PublishTime.Format("2006-01-02 15:04"),
	//			Topic:         video.Topic,
	//		}
	//		//if _, ok := favoriteMap[video.ID]; ok {
	//		//	vv.IsFavorite = true
	//		//}
	//		vv.IsFavorite = true
	//		videoResponse = append(videoResponse, vv)
	//	} else {
	//		err := errors.New(constant.VideoServerBug)
	//		zap.L().Error(err.Error())
	//		return nil, err
	//	}
	//}
	return videoResponse, nil
}
