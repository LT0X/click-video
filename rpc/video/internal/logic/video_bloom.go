package logic

import (
	"context"

	"douyin/rpc/video/internal/cache"
	"douyin/rpc/video/internal/model"
	"douyin/rpc/video/internal/svc"
)

func resolveExistingVideoIDs(ctx context.Context, svcCtx *svc.ServiceContext, ids []uint64) ([]uint64, error) {
	return resolveVideoIDs(ids, cache.VideoExists, func(missing []uint64) ([]uint64, error) {
		existing := make([]uint64, 0, len(missing))
		if len(missing) == 0 {
			return existing, nil
		}
		err := svcCtx.DBList.Mysql.WithContext(ctx).Model(&model.Video{}).
			Select("id").Where("id IN ?", missing).Find(&existing).Error
		return existing, err
	}, cache.AddVideoID)
}

func videoExists(ctx context.Context, svcCtx *svc.ServiceContext, id uint64) (bool, error) {
	ids, err := resolveExistingVideoIDs(ctx, svcCtx, []uint64{id})
	return len(ids) > 0, err
}

// resolveVideoIDs 将本机 Bloom 命中作为快速路径；miss 批量回源 DB 并修复本机过滤器，避免多副本间新 ID 短暂误拒。
func resolveVideoIDs(
	ids []uint64,
	bloomHit func(uint64) bool,
	query func([]uint64) ([]uint64, error),
	add func(uint64),
) ([]uint64, error) {
	exists := make(map[uint64]struct{}, len(ids))
	missing := make([]uint64, 0, len(ids))
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if bloomHit(id) {
			exists[id] = struct{}{}
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		found, err := query(missing)
		if err != nil {
			return nil, err
		}
		for _, id := range found {
			exists[id] = struct{}{}
			add(id)
		}
	}
	resolved := make([]uint64, 0, len(exists))
	for _, id := range ids {
		if _, ok := exists[id]; ok {
			resolved = append(resolved, id)
		}
	}
	return resolved, nil
}
