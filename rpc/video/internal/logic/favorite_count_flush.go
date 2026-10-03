package logic

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"douyin/package/mq"
	"douyin/rpc/video/internal/cache"
	"douyin/rpc/video/internal/svc"

	"go.uber.org/zap"
)

const (
	favoriteCountFlushInterval = 10 * time.Second
	favoriteCountFlushBatch    = int64(1000)
	hotVideoDetectInterval     = time.Minute
)

type favoriteCountUpdate struct {
	VideoID uint64
	Count   int64
}

// ApplyFavoriteCountEvent 以 user.rpc 发布的版本顺序应用增量，快照用于单向校准。
func ApplyFavoriteCountEvent(event mq.FavoriteCountEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	switch event.Type {
	case mq.FavoriteCountSnapshot:
		return cache.SetFavoriteCountSnapshot(event)
	case mq.FavoriteCountDelta:
		count, err := cache.RecordFavoriteCountDelta(event)
		if err != nil {
			return err
		}
		zap.L().Debug("Redis 点赞计数已原子更新", zap.Uint64("video_id", event.VideoID), zap.Int64("favorite_count", count))
		return nil
	default:
		return fmt.Errorf("未知点赞计数事件类型: %q", event.Type)
	}
}

func buildFavoriteCountUpdateSQL(updates []favoriteCountUpdate) (string, []interface{}) {
	if len(updates) == 0 {
		return "", nil
	}
	sorted := append([]favoriteCountUpdate(nil), updates...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].VideoID < sorted[j].VideoID })

	var query strings.Builder
	query.WriteString("UPDATE video SET favorite_count = CASE id")
	args := make([]interface{}, 0, len(sorted)*4)
	for _, update := range sorted {
		query.WriteString(" WHEN ? THEN ?")
		args = append(args, update.VideoID, update.Count)
	}
	query.WriteString(" ELSE favorite_count END WHERE id IN (")
	for i, update := range sorted {
		if i != 0 {
			query.WriteString(", ")
		}
		query.WriteString("?")
		args = append(args, update.VideoID)
	}
	query.WriteString(")")
	return query.String(), args
}

// FlushDirtyFavoriteCounts 只刷入视频计数服务自己的 video 表。
func FlushDirtyFavoriteCounts(ctx context.Context, svcCtx *svc.ServiceContext) error {
	values, err := cache.PopDirtyFavoriteVideoIDs(ctx, favoriteCountFlushBatch)
	if err != nil || len(values) == 0 {
		return err
	}

	videoIDs := make([]uint64, 0, len(values))
	for _, value := range values {
		videoID, parseErr := strconv.ParseUint(value, 10, 64)
		if parseErr != nil || videoID == 0 {
			zap.L().Warn("点赞脏视频集合包含无效 ID，已丢弃", zap.String("value", value))
			continue
		}
		videoIDs = append(videoIDs, videoID)
	}
	if len(videoIDs) == 0 {
		return nil
	}

	counts, missing, err := cache.ReadDirtyFavoriteCounts(videoIDs)
	if err != nil {
		if retryErr := cache.RequeueDirtyFavoriteVideoIDs(videoIDs); retryErr != nil {
			return fmt.Errorf("读取脏计数失败且回灌集合失败: %w", retryErr)
		}
		return err
	}
	updates := make([]favoriteCountUpdate, 0, len(counts))
	for videoID, count := range counts {
		updates = append(updates, favoriteCountUpdate{VideoID: videoID, Count: count})
	}
	if len(updates) > 0 {
		query, args := buildFavoriteCountUpdateSQL(updates)
		if err := svcCtx.DBList.Mysql.WithContext(ctx).Exec(query, args...).Error; err != nil {
			failedIDs := make([]uint64, 0, len(updates)+len(missing))
			for _, update := range updates {
				failedIDs = append(failedIDs, update.VideoID)
			}
			failedIDs = append(failedIDs, missing...)
			if retryErr := cache.RequeueDirtyFavoriteVideoIDs(failedIDs); retryErr != nil {
				return fmt.Errorf("批量刷盘失败且回灌脏集合失败: %w", retryErr)
			}
			return fmt.Errorf("CASE WHEN 批量刷盘失败: %w", err)
		}
		if err := cache.ExpireFavoriteCountsIfClean(videoIDs); err != nil {
			zap.L().Warn("点赞计数已落库，但设置缓存 TTL 失败", zap.Error(err))
		}
	}
	if err := cache.RequeueDirtyFavoriteVideoIDs(missing); err != nil {
		return fmt.Errorf("Redis 点赞计数缺失，回灌脏集合失败: %w", err)
	}
	return nil
}

func RunFavoriteCounterWorkers(ctx context.Context, svcCtx *svc.ServiceContext) {
	go runFavoriteCounterWorker(ctx, favoriteCountFlushInterval, func() error {
		return FlushDirtyFavoriteCounts(ctx, svcCtx)
	})
	go runFavoriteCounterWorker(ctx, hotVideoDetectInterval, func() error {
		return cache.DetectHotVideos(ctx)
	})
}

func runFavoriteCounterWorker(ctx context.Context, interval time.Duration, job func() error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := job(); err != nil {
				zap.L().Error("运行点赞计数后台任务失败", zap.Error(err))
			}
		}
	}
}
