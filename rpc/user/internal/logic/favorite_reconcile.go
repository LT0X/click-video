package logic

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"time"

	"douyin/package/mq"
	"douyin/package/util"
	"douyin/rpc/user/internal/model"
	"douyin/rpc/user/internal/svc"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type favoriteVideoCountRow struct {
	VideoID uint64 `gorm:"column:video_id"`
	Count   int64  `gorm:"column:count"`
}

type favoriteVideoCountVersionRow struct {
	VideoID      uint64 `gorm:"column:video_id"`
	CountVersion uint64 `gorm:"column:count_version"`
}

type favoriteVideoCountSnapshot struct {
	VideoID      uint64
	Count        int64
	CountVersion uint64
}

// buildFavoriteCountSnapshots 将 favorite 表计数与状态表版本合并，确保归零视频也会校准。
func buildFavoriteCountSnapshots(counts []favoriteVideoCountRow, versions []favoriteVideoCountVersionRow) []favoriteVideoCountSnapshot {
	countByVideoID := make(map[uint64]int64, len(counts))
	for _, row := range counts {
		if row.VideoID != 0 && row.Count >= 0 {
			countByVideoID[row.VideoID] = row.Count
		}
	}
	versionByVideoID := make(map[uint64]uint64, len(versions))
	for _, row := range versions {
		if row.VideoID != 0 {
			versionByVideoID[row.VideoID] = row.CountVersion
			if _, exists := countByVideoID[row.VideoID]; !exists {
				countByVideoID[row.VideoID] = 0
			}
		}
	}

	result := make([]favoriteVideoCountSnapshot, 0, len(countByVideoID))
	for videoID, count := range countByVideoID {
		result = append(result, favoriteVideoCountSnapshot{
			VideoID: videoID, Count: count, CountVersion: versionByVideoID[videoID],
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].VideoID < result[j].VideoID })
	return result
}

// ReconcileFavoriteCounts 只从 user.rpc 的 favorite 表读取真相，并通过快照事件交给 video.rpc 落地。
func ReconcileFavoriteCounts(ctx context.Context, svcCtx *svc.ServiceContext, broker *mq.FavoriteEventBroker) error {
	var counts []favoriteVideoCountRow
	var versions []favoriteVideoCountVersionRow
	// 在同一只读事务中读取关系计数和计数版本，避免快照拿到跨提交的混合状态。
	err := svcCtx.DBList.Mysql.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.Favorite{}).
			Select("video_id, COUNT(*) AS count").Group("video_id").Scan(&counts).Error; err != nil {
			return fmt.Errorf("按视频汇总 favorite 记录失败: %w", err)
		}
		if err := tx.Model(&model.FavoriteVideoCountState{}).
			Select("video_id, count_version").Find(&versions).Error; err != nil {
			return fmt.Errorf("读取点赞计数版本状态失败: %w", err)
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}

	for _, snapshot := range buildFavoriteCountSnapshots(counts, versions) {
		eventID, err := util.GetSonyFlakeID()
		if err != nil {
			return fmt.Errorf("生成点赞对账事件 ID 失败: %w", err)
		}
		event := mq.FavoriteCountEvent{
			EventID: eventID,
			Type:    mq.FavoriteCountSnapshot,
			VideoID: snapshot.VideoID,
			Version: snapshot.CountVersion,
			Count:   snapshot.Count,
		}
		if err := broker.PublishFavoriteCount(event); err != nil {
			return fmt.Errorf("发布 video=%s 的点赞对账快照失败: %w", strconv.FormatUint(snapshot.VideoID, 10), err)
		}
	}
	return nil
}

func RunFavoriteCountReconciliation(ctx context.Context, svcCtx *svc.ServiceContext, broker *mq.FavoriteEventBroker, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	reconcile := func() {
		if err := ReconcileFavoriteCounts(ctx, svcCtx, broker); err != nil {
			zap.L().Error("点赞计数对账失败", zap.Error(err))
		}
	}
	reconcile()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconcile()
		}
	}
}
