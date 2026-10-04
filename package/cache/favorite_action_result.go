package cache

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

const favoriteActionResultWaitTimeout = time.Second

func favoriteActionResultKey(eventID uint64) string {
	return fmt.Sprintf("favorite_action_result:%d", eventID)
}

func parseFavoriteActionCount(value string) (int64, bool) {
	count, err := strconv.ParseInt(value, 10, 64)
	if err != nil || count < 0 {
		return 0, false
	}
	return count, true
}

// WaitFavoriteActionCount 有界等待 user.rpc 提交事务后的计数；超时不影响点赞成功响应。
func WaitFavoriteActionCount(eventID uint64) (int64, bool) {
	if eventID == 0 || UserRedisClient == nil {
		return 0, false
	}
	result, err := UserRedisClient.BLPop(favoriteActionResultWaitTimeout, favoriteActionResultKey(eventID)).Result()
	if err != nil {
		if err != redis.Nil {
			zap.L().Warn("读取点赞动作计数结果失败", zap.Error(err))
		}
		return 0, false
	}
	if len(result) != 2 {
		return 0, false
	}
	count, ok := parseFavoriteActionCount(result[1])
	if !ok {
		zap.L().Warn("点赞动作计数结果格式无效")
	}
	return count, ok
}
