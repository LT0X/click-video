package cache

import (
	"douyin/model"
	"douyin/package/constant"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

// InvalidateCommentCache 删除评论列表缓存，不删除点赞计数所在的 Hash，避免丢失视频服务维护的点赞字段。
func InvalidateCommentCache(videoID uint64) error {
	if videoID == 0 || CommentRedisClient == nil || VideoRedisClient == nil {
		return fmt.Errorf("评论缓存失效参数或 Redis 客户端无效")
	}
	key := constant.CommentPrefix + strconv.FormatUint(videoID, 10)
	if err := CommentRedisClient.Del(key).Err(); err != nil {
		return fmt.Errorf("删除视频 %d 的评论缓存失败: %w", videoID, err)
	}
	// 只删评论计数字段，保留 video.rpc 异步维护的点赞计数。
	countKey := constant.VideoInfoCountPrefix + strconv.FormatUint(videoID, 10)
	if err := VideoRedisClient.HDel(countKey, constant.CommentCountField).Err(); err != nil {
		return fmt.Errorf("删除视频 %d 的评论计数缓存失败: %w", videoID, err)
	}
	return nil
}

// 评论增加 会影响视频的评论数 和评论表 需要lua脚本保证原子性 （目前采取删缓存）
// 评论列表zset吧 按照评论时间排序（可以考虑时间加赞数加权排序）
func CommentAdd(c *model.Comment) error {
	zsetKey := constant.CommentPrefix + strconv.FormatUint(c.VideoID, 10)
	// 应该删缓存 而不是增加 有过期时间的 过期了怎么办
	// 更新倒是可以考虑 但是可能有数据不一致的情况
	err := CommentRedisClient.Del(zsetKey).Err()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	videoCountKey := constant.VideoInfoCountPrefix + strconv.FormatUint(c.VideoID, 10)
	err = VideoRedisClient.Del(videoCountKey).Err()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	return nil
}

// lua脚本保证原子性 （目前采取删缓存）
func CommentDelete(c *model.Comment) error {
	zsetKey := constant.CommentPrefix + strconv.FormatUint(c.VideoID, 10)
	dataJSON, err := json.Marshal(c)
	if err != nil {
		return err
	}
	err = CommentRedisClient.ZRem(zsetKey, dataJSON).Err()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	// 减少视频的评论数
	videoCountKey := constant.VideoInfoCountPrefix + strconv.FormatUint(c.VideoID, 10)
	err = VideoRedisClient.Del(videoCountKey).Err()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	return nil
}

func SetComments(videoID uint64, comments []*model.Comment) error {
	zsetKey := constant.CommentPrefix + strconv.FormatUint(videoID, 10)
	members := make([]redis.Z, 0, len(comments))
	for _, c := range comments {
		dataJSON, err := json.Marshal(c)
		if err != nil {
			zap.L().Error(err.Error())
			return err
		}
		member := redis.Z{
			Score:  float64(c.CreatedTime.UnixMilli()),
			Member: dataJSON,
		}
		members = append(members, member)
	}
	pp := CommentRedisClient.Pipeline()
	pp.ZAdd(zsetKey, members...).Err()
	pp.Expire(zsetKey, constant.Expiration+time.Duration(rand.Intn(200))*time.Second)
	_, err := pp.Exec()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	return nil
}

func GetCommentsByVideoID(videoID uint64) ([]*model.Comment, error) {
	zsetKey := constant.CommentPrefix + strconv.FormatUint(videoID, 10)
	commentsJSON, err := CommentRedisClient.ZRevRange(zsetKey, 0, -1).Result()
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	// ZRange 查不到数据不会返回 redis.Nil
	if len(commentsJSON) == 0 {
		return nil, redis.Nil
	}
	comments := make([]*model.Comment, 0, len(commentsJSON))
	for _, id := range commentsJSON {
		var data model.Comment
		err = json.Unmarshal([]byte(id), &data)
		if err != nil {
			return nil, err
		}
		comments = append(comments, &data)
	}
	return comments, nil
}
