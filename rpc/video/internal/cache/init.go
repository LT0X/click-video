package cache

import (
	"douyin/model"
	"douyin/rpc/video/internal/svc"
	"fmt"
	"strconv"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

var UserRedisClient *redis.Client
var VideoRedisClient *redis.Client
var CommentRedisClient *redis.Client

var VideoIDBloomFilter *IDBloomFilter

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

	if err := initBloomFilter(ctx); err != nil {
		zap.L().Fatal("初始化 video ID 布隆过滤器失败", zap.Error(err))
	}
}

// 初始化布隆过滤器
// 布隆过滤器的预估元素数量 和误报率 决定了底层bitmap的大小 和 无偏哈希函数的个数
func initBloomFilter(ctx *svc.ServiceContext) error {
	VideoIDBloomFilter = NewIDBloomFilter(100000, 0.01)
	videoIDList := make([]uint64, 0)
	if err := ctx.DBList.Mysql.Model(&model.Video{}).Select("id").Find(&videoIDList).Error; err != nil {
		return fmt.Errorf("加载视频 ID 布隆过滤器数据失败: %w", err)
	}
	for _, v := range videoIDList {
		VideoIDBloomFilter.AddString(strconv.FormatUint(v, 10))
	}
	zap.L().Info("初始化布隆过滤器: 成功")
	return nil
}
