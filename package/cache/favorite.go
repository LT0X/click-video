package cache

import (
	"douyin/package/constant"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

func SetFavoriteSet(userID uint64, favoriteIDSet []uint64) error {
	key := constant.FavoriteIDPrefix + strconv.FormatUint(userID, 10)
	favoriteIDStrings := make([]string, 1, len(favoriteIDSet)+1)
	favoriteIDStrings[0] = "0"
	for i := range favoriteIDSet {
		favoriteIDStrings = append(favoriteIDStrings, strconv.FormatUint(favoriteIDSet[i], 10))
	}
	pp := UserRedisClient.Pipeline()
	pp.SAdd(key, favoriteIDStrings)
	pp.Expire(key, constant.Expiration+time.Duration(rand.Intn(100))*time.Second)
	_, err := pp.Exec()
	return err
}

func GetFavoriteSet(userID uint64) ([]uint64, error) {
	key := constant.FavoriteIDPrefix + strconv.FormatUint(userID, 10)
	// 若key不存在会返回空集合
	idSet, err := UserRedisClient.SMembers(key).Result()
	if err != nil {
		zap.L().Error(err.Error())
		return nil, err
	}
	if len(idSet) == 0 {
		return nil, redis.Nil
	}
	res := make([]uint64, 0, len(idSet))
	for _, t := range idSet {
		id, err := strconv.ParseUint(t, 10, 64)
		if err != nil {
			zap.L().Error(err.Error())
			return nil, err
		}
		res = append(res, id)
	}
	return res, nil
}
