package cache

import (
	"douyin/rpc/user/internal/svc"
	"fmt"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

var UserRedisClient *redis.Client
var VideoRedisClient *redis.Client
var CommentRedisClient *redis.Client

func InitRedis(ctx *svc.ServiceContext) {
	// userRedis 连接
	UserRedisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", ctx.Config.DBList.Redis.Host, ctx.Config.DBList.Redis.Port),
		Password: ctx.Config.DBList.Redis.Password,
		DB:       0,
		PoolSize: ctx.Config.DBList.Redis.PoolSize, //每个CPU最大连接数
	})
	_, err := UserRedisClient.Ping().Result()
	if err != nil {
		zap.L().Fatal("user_redis连接失败", zap.Error(err))
	}
	// videoRedis 连接
	VideoRedisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", ctx.Config.DBList.Redis.Host, ctx.Config.DBList.Redis.Port),
		Password: ctx.Config.DBList.Redis.Password,
		DB:       1,
		PoolSize: ctx.Config.DBList.Redis.PoolSize, //每个CPU最大连接数
	})
	_, err = VideoRedisClient.Ping().Result()
	if err != nil {
		zap.L().Fatal("video_redis连接失败", zap.Error(err))
	}
	// videoRedis 连接
	CommentRedisClient = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", ctx.Config.DBList.Redis.Host, ctx.Config.DBList.Redis.Port),
		Password: ctx.Config.DBList.Redis.Password,
		DB:       2,
		PoolSize: ctx.Config.DBList.Redis.PoolSize, //每个CPU最大连接数每个CPU最大连接数
	})
	_, err = VideoRedisClient.Ping().Result()
	if err != nil {
		zap.L().Fatal("comment_redis连接失败", zap.Error(err))
	}
	zap.L().Info("redis连接: 成功")

}
