package cache

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"douyin/package/constant"
	"douyin/package/metrics"
	"douyin/package/mq"
	"douyin/rpc/video/internal/model"

	"github.com/go-redis/redis"
	"go.uber.org/zap"
)

const (
	favoriteCountDirtyVideoSet = "favorite_count_dirty_video_ids"
	favoriteCountEventPrefix   = "favorite_count_event:"
	favoriteCountVersionKey    = "favorite_count_version"
	videoAccessMinutePrefix    = "video_access_minute:"
	hotVideoPrefix             = "hot_video:"
	videoAccessThreshold       = int64(1000)
)

const favoriteCountDeltaLua = `
if redis.call('EXISTS', KEYS[3]) == 1 then
  return tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
end
local currentVersion = tonumber(redis.call('HGET', KEYS[4], ARGV[4]) or '0')
local incomingVersion = tonumber(ARGV[6])
if incomingVersion < currentVersion then
  redis.call('SET', KEYS[3], '1', 'NX', 'EX', ARGV[3])
  return tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
end
local countValue = redis.call('HGET', KEYS[1], ARGV[1])
local count
if incomingVersion == currentVersion then
  -- 版本已写但前次脚本后续命令失败时，重放用绝对计数修复局部写入。
  count = tonumber(ARGV[5])
  redis.call('HSET', KEYS[1], ARGV[1], count)
elseif countValue and incomingVersion == currentVersion + 1 then
  count = redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
  if tonumber(count) ~= tonumber(ARGV[5]) then
    count = tonumber(ARGV[5])
    redis.call('HSET', KEYS[1], ARGV[1], count)
  end
else
  count = tonumber(ARGV[5])
  redis.call('HSET', KEYS[1], ARGV[1], count)
end
redis.call('HSET', KEYS[4], ARGV[4], incomingVersion)
redis.call('SADD', KEYS[2], ARGV[4])
redis.call('PERSIST', KEYS[1])
-- 去重标记最后写，避免前序状态写入失败后重投被误判成已处理。
redis.call('SET', KEYS[3], '1', 'NX', 'EX', ARGV[3])
return count
`

var favoriteCountDeltaScript = redis.NewScript(favoriteCountDeltaLua)

const favoriteCountSnapshotLua = `
local currentVersion = tonumber(redis.call('HGET', KEYS[3], ARGV[4]) or '0')
local incomingVersion = tonumber(ARGV[3])
if incomingVersion < currentVersion then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[3], ARGV[4], incomingVersion)
redis.call('SADD', KEYS[2], ARGV[4])
redis.call('PERSIST', KEYS[1])
return 1
`

var favoriteCountSnapshotScript = redis.NewScript(favoriteCountSnapshotLua)

const favoriteCountBackfillLua = `
if redis.call('SISMEMBER', KEYS[2], ARGV[4]) == 1 then
  return 0
end
local inserted = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])
if inserted == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[3])
end
return inserted
`

const favoriteCountExpireIfCleanLua = `
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  local ttl = redis.call('TTL', KEYS[2])
  if ttl < tonumber(ARGV[2]) then
    return redis.call('EXPIRE', KEYS[2], ARGV[2])
  end
end
return 0
`

const extendCacheTTLLua = `
local ttl = redis.call('TTL', KEYS[1])
if ttl < tonumber(ARGV[1]) then
  return redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return 0
`

func favoriteCountKey(videoID uint64) string {
	return constant.VideoInfoCountPrefix + strconv.FormatUint(videoID, 10)
}

func favoriteCountTTL() time.Duration {
	return constant.Expiration + time.Duration(rand.Intn(100))*time.Second
}

func RecordFavoriteCountDelta(event mq.FavoriteCountEvent) (int64, error) {
	videoID := strconv.FormatUint(event.VideoID, 10)
	result, err := favoriteCountDeltaScript.Run(VideoRedisClient,
		[]string{
			favoriteCountKey(event.VideoID),
			favoriteCountDirtyVideoSet,
			favoriteCountEventPrefix + strconv.FormatUint(event.EventID, 10),
			favoriteCountVersionKey,
		},
		constant.FavoritedCountField,
		event.Delta,
		int64((7*24*time.Hour + time.Duration(rand.Intn(24))*time.Hour).Seconds()),
		videoID,
		event.Count,
		event.Version,
	).Int64()
	if err != nil {
		return 0, fmt.Errorf("Redis 原子更新点赞计数失败: %w", err)
	}
	return result, nil
}

func SetFavoriteCountSnapshot(event mq.FavoriteCountEvent) error {
	videoID := strconv.FormatUint(event.VideoID, 10)
	if _, err := favoriteCountSnapshotScript.Run(VideoRedisClient,
		[]string{favoriteCountKey(event.VideoID), favoriteCountDirtyVideoSet, favoriteCountVersionKey},
		constant.FavoritedCountField,
		event.Count,
		event.Version,
		videoID,
	).Result(); err != nil {
		return fmt.Errorf("Redis 覆盖点赞计数快照失败: %w", err)
	}
	return nil
}

// LoadFavoriteCounts 优先用 Hash 批量读取计数，miss 回源值异步回填。
func LoadFavoriteCounts(ctx context.Context, videos []*model.Video) error {
	if len(videos) == 0 {
		return nil
	}
	startedAt := time.Now()
	defer func() {
		metrics.Default.ObserveFavoriteCountLookup(time.Since(startedAt))
	}()
	pipe := VideoRedisClient.Pipeline()
	commands := make([]*redis.StringCmd, len(videos))
	for i, video := range videos {
		commands[i] = pipe.HGet(favoriteCountKey(video.ID), constant.FavoritedCountField)
	}
	_, pipelineErr := pipe.Exec()
	if pipelineErr != nil && pipelineErr != redis.Nil {
		zap.L().Warn("Pipeline 读取视频点赞计数失败，使用数据库值", zap.Error(pipelineErr))
	}

	misses := make([]*model.Video, 0)
	for i, command := range commands {
		count, found, outcome := recordFavoriteCountCacheAccess(metrics.Default, command.Val(), command.Err())
		if found {
			videos[i].FavoriteCount = count
			continue
		}
		if outcome == "miss" {
			misses = append(misses, videos[i])
			continue
		}
		if command.Err() != nil {
			zap.L().Warn("读取视频点赞缓存失败，使用数据库值", zap.Uint64("video_id", videos[i].ID), zap.Error(command.Err()))
			continue
		}
		zap.L().Warn("视频点赞缓存格式错误，使用数据库值", zap.Uint64("video_id", videos[i].ID), zap.String("value", command.Val()))
	}
	if len(misses) > 0 {
		go backfillFavoriteCounts(misses)
	}
	videoIDs := make([]uint64, 0, len(videos))
	for _, video := range videos {
		videoIDs = append(videoIDs, video.ID)
	}
	if err := RecordVideoAccess(ctx, videoIDs); err != nil {
		zap.L().Warn("记录视频访问频次失败", zap.Error(err))
	}
	return nil
}

func recordFavoriteCountCacheAccess(registry *metrics.Registry, value string, commandErr error) (int64, bool, string) {
	if commandErr == redis.Nil {
		registry.ObserveFavoriteCountCacheAccess("miss")
		return 0, false, "miss"
	}
	if commandErr != nil {
		registry.ObserveFavoriteCountCacheAccess("error")
		return 0, false, "error"
	}
	count, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		registry.ObserveFavoriteCountCacheAccess("error")
		return 0, false, "error"
	}
	registry.ObserveFavoriteCountCacheAccess("hit")
	return count, true, "hit"
}

func backfillFavoriteCounts(videos []*model.Video) {
	pipe := VideoRedisClient.Pipeline()
	commands := make([]*redis.Cmd, 0, len(videos))
	for _, video := range videos {
		ttl := int64(favoriteCountTTL().Seconds())
		commands = append(commands, pipe.Eval(favoriteCountBackfillLua,
			[]string{favoriteCountKey(video.ID), favoriteCountDirtyVideoSet},
			constant.FavoritedCountField,
			video.FavoriteCount,
			ttl,
			strconv.FormatUint(video.ID, 10),
		))
	}
	_, err := pipe.Exec()
	if err != nil && err != redis.Nil {
		zap.L().Warn("Pipeline 异步回填视频点赞缓存失败", zap.Error(err))
	}
	for _, command := range commands {
		if command.Err() != nil && command.Err() != redis.Nil {
			zap.L().Warn("异步回填视频点赞缓存失败", zap.Error(command.Err()))
		}
	}
}

func RecordVideoAccess(ctx context.Context, videoIDs []uint64) error {
	if len(videoIDs) == 0 {
		return nil
	}
	minuteKey := videoAccessMinutePrefix + time.Now().Format("200601021504")
	pipe := VideoRedisClient.Pipeline()
	for _, videoID := range videoIDs {
		pipe.ZIncrBy(minuteKey, 1, strconv.FormatUint(videoID, 10))
	}
	pipe.Expire(minuteKey, 2*time.Minute+time.Duration(rand.Intn(30))*time.Second)
	_, err := pipe.Exec()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("写入视频分钟访问 ZSet 失败: %w", err)
	}
	return nil
}

func DetectHotVideos(ctx context.Context) error {
	previousMinute := videoAccessMinutePrefix + time.Now().Add(-time.Minute).Format("200601021504")
	hotVideos, err := VideoRedisClient.ZRangeByScoreWithScores(previousMinute, redis.ZRangeBy{
		Min: strconv.FormatInt(videoAccessThreshold+1, 10),
		Max: "+inf",
	}).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("查询上一分钟热点视频失败: %w", err)
	}
	if len(hotVideos) == 0 {
		return nil
	}

	pipe := VideoRedisClient.Pipeline()
	for _, item := range hotVideos {
		videoID, parseErr := strconv.ParseUint(fmt.Sprint(item.Member), 10, 64)
		if parseErr != nil || videoID == 0 {
			continue
		}
		markerTTL := 5*time.Minute + time.Duration(rand.Intn(60))*time.Second
		cacheTTL := 10*time.Minute + time.Duration(rand.Intn(120))*time.Second
		id := strconv.FormatUint(videoID, 10)
		pipe.Set(hotVideoPrefix+id, "1", markerTTL)
		pipe.Eval(extendCacheTTLLua, []string{constant.VideoInfoPrefix + id}, int64(cacheTTL.Seconds()))
		pipe.Eval(favoriteCountExpireIfCleanLua,
			[]string{favoriteCountDirtyVideoSet, favoriteCountKey(videoID)},
			id, int64(cacheTTL.Seconds()))
	}
	_, err = pipe.Exec()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("延长热点视频缓存 TTL 失败: %w", err)
	}
	return nil
}

func PopDirtyFavoriteVideoIDs(ctx context.Context, count int64) ([]string, error) {
	ids, err := VideoRedisClient.SPopN(favoriteCountDirtyVideoSet, count).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("获取点赞脏视频集合失败: %w", err)
	}
	return ids, nil
}

func RequeueDirtyFavoriteVideoIDs(videoIDs []uint64) error {
	if len(videoIDs) == 0 {
		return nil
	}
	values := make([]interface{}, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		values = append(values, strconv.FormatUint(videoID, 10))
	}
	return VideoRedisClient.SAdd(favoriteCountDirtyVideoSet, values...).Err()
}

func ReadDirtyFavoriteCounts(videoIDs []uint64) (map[uint64]int64, []uint64, error) {
	pipe := VideoRedisClient.Pipeline()
	commands := make(map[uint64]*redis.StringCmd, len(videoIDs))
	for _, videoID := range videoIDs {
		commands[videoID] = pipe.HGet(favoriteCountKey(videoID), constant.FavoritedCountField)
	}
	_, pipelineErr := pipe.Exec()
	if pipelineErr != nil && pipelineErr != redis.Nil {
		return nil, videoIDs, fmt.Errorf("Pipeline 读取点赞脏计数失败: %w", pipelineErr)
	}
	counts := make(map[uint64]int64, len(videoIDs))
	missing := make([]uint64, 0)
	for videoID, command := range commands {
		count, err := strconv.ParseInt(command.Val(), 10, 64)
		if command.Err() == nil && err == nil {
			counts[videoID] = count
			continue
		}
		missing = append(missing, videoID)
	}
	return counts, missing, nil
}

func ExpireFavoriteCountsIfClean(videoIDs []uint64) error {
	if len(videoIDs) == 0 {
		return nil
	}
	pipe := VideoRedisClient.Pipeline()
	for _, videoID := range videoIDs {
		id := strconv.FormatUint(videoID, 10)
		pipe.Eval(favoriteCountExpireIfCleanLua,
			[]string{favoriteCountDirtyVideoSet, favoriteCountKey(videoID)},
			id, int64(favoriteCountTTL().Seconds()))
	}
	_, err := pipe.Exec()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("设置点赞计数缓存 TTL 失败: %w", err)
	}
	return nil
}
