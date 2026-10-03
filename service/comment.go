package service

import (
	"context"
	"douyin/database"
	"douyin/model"
	"douyin/package/cache"
	"douyin/package/constant"
	"douyin/package/mq"
	"douyin/package/util"
	"douyin/response"
	"douyin/rpc/video/video"
	"sync/atomic"

	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gofrs/uuid"
	"go.uber.org/zap"
)

type CommentService struct {
	// 1-发布评论，2-删除评论
	ActionType string `query:"action_type"`
	// 要删除的评论id，在action_type=2的时候使用
	CommentID *string `query:"comment_id,omitempty"`
	// 用户填写的评论内容，在action_type=1的时候使用
	CommentText *string `query:"comment_text,omitempty"`
	// 用户鉴权token
	Token string `query:"token"`
	// 视频id
	VideoID uint64 `query:"video_id"`
}

func (service *CommentService) PostComment(userID uint64) (*response.CommentActionResponse, error) {
	// TODO 增加敏感词过滤 可以异步实现 comment表多一列屏蔽信息
	if service.CommentText == nil || *service.CommentText == "" || utf8.RuneCountInString(*service.CommentText) > 255 {
		return nil, errors.New(constant.BadParaRequest)
	}
	// 评论异步入队前通过 video.rpc 校验视频，避免无效视频的评论被成功应答后再进入死信队列。
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	videoResp, err := database.RPC.VideoRpc.SelectVideoListByVideoID(ctx, &video.SelectVideoListByVideoIDRequest{
		VideoIDList: []uint64{service.VideoID},
	})
	if err != nil {
		return nil, err
	}
	if videoResp == nil || len(videoResp.Videos) == 0 {
		return nil, errors.New(constant.BloomFilterRejected)
	}
	id, err := util.GetSonyFlakeID()
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	// 消息队列异步处理评论信息
	msg := &model.Comment{
		ID:          id,
		VideoID:     service.VideoID,
		UserID:      userID,
		Content:     *service.CommentText,
		CreatedTime: time.Now(),
	}
	err = mq.SendCommentMessage(msg)
	if err != nil {
		return nil, err
	}
	// 查找评论的用户信息
	user, err := cache.GetUserInfo(userID)
	if err != nil {
		zap.L().Sugar().Warn(constant.CacheMiss)
		user, err = database.SelectUserByID(userID)
		if err != nil {
			zap.L().Error(err.Error())
			return nil, err
		}
		// 设置缓存
		err := cache.SetUserInfo(user)
		if err != nil {
			zap.L().Sugar().Error(constant.SetCacheError)
		}
	}
	return &response.CommentActionResponse{
		StatusCode: response.Success,
		StatusMsg:  constant.CommentSuccess,
		Comment:    response.BuildComment(msg, user, true),
	}, nil
}

func (service *CommentService) DeleteComment(userID uint64) (*response.CommentActionResponse, error) {
	// video.rpc 在本服务事务内删除评论、维护计数并写入缓存失效 Outbox。
	//msg, err := database.CommentDelete(service.CommentID, service.VideoID, userID)
	cid, _ := strconv.ParseUint(*service.CommentID, 10, 64)
	resp, err := database.RPC.VideoRpc.CommentDelete(context.TODO(), &video.CommentDeleteRequest{
		CommentID: cid,
		VideoID:   service.VideoID,
		UserID:    userID,
	})
	if err != nil {
		return nil, err
	}
	msg := model.TransformComment(resp.Comment)
	// 查找评论的用户信息
	user, err := cache.GetUserInfo(userID)
	if err != nil {
		zap.L().Sugar().Warn(constant.CacheMiss)
		user, err = database.SelectUserByID(userID)
		if err != nil {
			zap.L().Error(err.Error())
			return nil, err
		}
		// 设置缓存
		err := cache.SetUserInfo(user)
		if err != nil {
			zap.L().Sugar().Error(constant.SetCacheError)
		}
	}
	return &response.CommentActionResponse{
		StatusCode: response.Success,
		StatusMsg:  constant.DeleteCommentSuccess,
		Comment:    response.BuildComment(msg, user, true),
	}, nil
}

func (service *CommentService) CommentList(userID uint64) (*response.CommentListResponse, error) {
	atomic.AddInt32(&Ppp, 1)
	fmt.Println(Ppp)
	// 先拿到这个视频的所有评论
	comments, err := cache.GetCommentsByVideoID(service.VideoID)
	if err != nil {
		zap.L().Sugar().Warn(constant.CacheMiss)
		// 加分布式锁 这里分布式锁严格测试过了 感觉没什么很大问题
		key := "lock:" + constant.CommentPrefix + strconv.FormatUint(service.VideoID, 10)
		value, err := uuid.NewV4()
		if err != nil {
			zap.L().Sugar().Error(err)
			return nil, err
		}
		uuidValue := value.String()
		for ok, err := cache.GetLock(key, uuidValue, constant.LockTime, cache.CommentRedisClient); err == nil; {
			if ok {
				defer cache.ReleaseLock(key, uuidValue, cache.VideoRedisClient)

				//comments, err = database.GetCommentsByVideoIDFromMaster(service.VideoID)
				resp, err := database.RPC.VideoRpc.GetCommentsByVideoIDFromMaster(context.TODO(), &video.GetCommentsByVideoIDFromMasterRequest{
					VideoID: service.VideoID,
				})
				if err != nil {
					zap.L().Sugar().Error(err)
					return nil, err
				}
				if resp == nil {
					return nil, errors.New("video.rpc 返回空评论响应")
				}
				comments = model.TransformComments(resp.Comment)

				atomic.AddInt32(&CheckDB, 1)
				err = cache.SetComments(service.VideoID, comments)
				if err != nil {
					zap.L().Error(err.Error())
				}
				break
			}
			time.Sleep(constant.RetryTime * time.Millisecond)
			comments, err = cache.GetCommentsByVideoID(service.VideoID)
			if err == nil {
				break
			}
		}
		if err != nil {
			zap.L().Sugar().Error(err)
			return nil, err
		}
	}
	// TODO 这里应该先去redis里拿评论的作者信息 逻辑太复杂
	// redis没有的再去数据库评论的作者信息
	userIDs := make([]uint64, 0, len(comments))
	for _, cc := range comments {
		userIDs = append(userIDs, cc.UserID)
	}
	// 范围查询
	users, err := database.SelectUserListByIDs(userIDs)
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	// 判断这些用户是否被关注
	followingIDs := make([]uint64, 0)
	if userID != 0 {
		followingIDs, err = cache.GetFollowUserIDSet(userID)
		if err != nil {
			zap.L().Sugar().Warn(constant.CacheMiss)
			followingIDs, err = database.SelectFollowingByUserID(userID)
			if err != nil {
				zap.L().Error(err.Error())
				return nil, err
			}
			// 缓存未命中设置缓存
			go func() {
				err = cache.SetFollowUserIDSet(userID, followingIDs)
				if err != nil {
					zap.L().Error(err.Error())
				}
			}()
		}
	}
	followingMap := make(map[uint64]struct{}, len(followingIDs))
	for _, ff := range followingIDs {
		followingMap[ff] = struct{}{}
	}
	// 再把user放到map里 避免双重循环去查找F
	usersMap := make(map[uint64]*model.User, len(users))
	for i, user := range users {
		usersMap[user.ID] = &users[i]
	}
	// 构造返回值
	commentResponse := make([]response.Comment, 0, len(comments))
	for _, cc := range comments {
		_, isFollowed := followingMap[cc.UserID]
		res := response.BuildComment(cc, usersMap[cc.UserID], isFollowed)
		commentResponse = append(commentResponse, *res)
	}
	return &response.CommentListResponse{
		StatusCode:  response.Success,
		StatusMsg:   constant.LoadCommentsSuccess,
		CommentList: commentResponse,
	}, nil
}

var Ppp int32
var CheckDB int32
