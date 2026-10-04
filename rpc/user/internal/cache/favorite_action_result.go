package cache

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

const favoriteActionResultTTL = 30 * time.Second

func favoriteActionResultKey(eventID uint64) string {
	return fmt.Sprintf("favorite_action_result:%d", eventID)
}

// SetFavoriteActionResult 临时保存已提交动作的计数，供网关按 EventID 取回。
func SetFavoriteActionResult(eventID uint64, count int64) error {
	if eventID == 0 || count < 0 {
		return errors.New("点赞动作结果缺少有效事件 ID 或计数")
	}
	if UserRedisClient == nil {
		return errors.New("user Redis 客户端未初始化")
	}
	key := favoriteActionResultKey(eventID)
	ttl := favoriteActionResultTTL + time.Duration(rand.Intn(10))*time.Second
	pipe := UserRedisClient.TxPipeline()
	pipe.LPush(key, strconv.FormatInt(count, 10))
	pipe.Expire(key, ttl)
	_, err := pipe.Exec()
	return err
}
