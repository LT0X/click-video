package logic

import (
	"context"
	"errors"
	"sort"

	"douyin/package/constant"
	"douyin/package/util"
	"douyin/rpc/user/internal/cache"
	"douyin/rpc/user/internal/model"
	"douyin/rpc/user/internal/svc"
	"douyin/rpc/user/user"

	"github.com/zeromicro/go-zero/core/logx"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FavoriteVideoLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewFavoriteVideoLogic(ctx context.Context, svcCtx *svc.ServiceContext) *FavoriteVideoLogic {
	return &FavoriteVideoLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *FavoriteVideoLogic) FavoriteVideo(in *user.FavoriteVideoRequest) (*user.FavoriteVideoResponse, error) {
	if in.UserID == 0 || in.VideoID == 0 || (in.Cnt != 1 && in.Cnt != -1) {
		return nil, errors.New(constant.BadParaRequest)
	}
	eventID, err := util.GetSonyFlakeID()
	if err != nil {
		return nil, err
	}
	sequence, err := ReserveFavoriteActionSequence(l.ctx, l.svcCtx, in.UserID, in.VideoID)
	if err != nil {
		return nil, err
	}
	changed, _, err := ApplyFavoriteAction(l.ctx, l.svcCtx, eventID, sequence, in.UserID, 0, in.VideoID, in.Cnt)
	if err != nil {
		return nil, err
	}
	if changed {
		return &user.FavoriteVideoResponse{Reply: 1}, nil
	}
	return &user.FavoriteVideoResponse{Reply: 0}, nil
}

// ApplyFavoriteAction 仅在 user.rpc 内事务化维护关系、用户计数和计数 Outbox。
func ApplyFavoriteAction(ctx context.Context, svcCtx *svc.ServiceContext, eventID, actionSequence, userID, authorID, videoID uint64, delta int64) (bool, int64, error) {
	if eventID == 0 || userID == 0 || videoID == 0 || (delta != 1 && delta != -1) {
		return false, 0, errors.New(constant.BadParaRequest)
	}
	favorite := model.Favorite{UserID: userID, VideoID: videoID}
	changed := false
	var favoriteCount int64
	err := svcCtx.DBList.Mysql.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 动作版本行与 favorite 状态同事务更新，防止多实例消费时旧动作覆盖新动作。
		actionState := model.FavoriteActionState{UserID: userID, VideoID: videoID}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&actionState).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("user_id = ? AND video_id = ?", userID, videoID).Take(&actionState).Error; err != nil {
			return err
		}
		if actionSequence == 0 {
			// 兼容升级时队列里遗留的旧格式消息：在 DB 行锁下分配序号，避免 ACK 丢弃。
			actionSequence = actionState.LastIssuedSequence + 1
			if actionSequence == 0 {
				return errors.New("点赞动作序号溢出")
			}
		}
		if actionSequence <= actionState.LastActionSequence {
			countState, err := lockFavoriteVideoCountState(tx, videoID)
			if err != nil {
				return err
			}
			favoriteCount = countState.FavoriteCount
			return nil
		}
		if actionSequence > actionState.LastIssuedSequence {
			actionState.LastIssuedSequence = actionSequence
		}
		if err := tx.Model(&actionState).Updates(map[string]interface{}{
			"last_issued_sequence": actionState.LastIssuedSequence,
			"last_action_sequence": actionSequence,
		}).Error; err != nil {
			return err
		}

		countState, err := lockFavoriteVideoCountState(tx, videoID)
		if err != nil {
			return err
		}

		// 锁定 user 表行，让同一用户的并发点赞动作串行修改其 favorite 关系与计数。
		userIDs := []uint64{userID}
		if authorID != 0 {
			userIDs = append(userIDs, authorID)
		}
		sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
		lockedUserID := uint64(0)
		for _, id := range userIDs {
			if id == lockedUserID {
				continue
			}
			var lockedUser model.User
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").First(&lockedUser, id).Error; err != nil {
				return err
			}
			lockedUserID = id
		}

		if delta == 1 {
			result := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "user_id"}, {Name: "video_id"}},
				DoNothing: true,
			}).Create(&favorite)
			if result.Error != nil {
				return result.Error
			}
			changed = result.RowsAffected == 1
		} else {
			result := tx.Where("user_id = ? AND video_id = ?", userID, videoID).Delete(&model.Favorite{})
			if result.Error != nil {
				return result.Error
			}
			changed = result.RowsAffected == 1
		}
		if !changed {
			favoriteCount = countState.FavoriteCount
			return nil
		}

		if err := tx.Model(&model.User{}).Where("id = ?", userID).
			UpdateColumn("favorite_count", gorm.Expr("GREATEST(favorite_count + ?, 0)", delta)).Error; err != nil {
			return err
		}
		if authorID != 0 {
			if err := tx.Model(&model.User{}).Where("id = ?", authorID).
				UpdateColumn("total_favorited", gorm.Expr("GREATEST(total_favorited + ?, 0)", delta)).Error; err != nil {
				return err
			}
		}

		countState.FavoriteCount += delta
		if countState.FavoriteCount < 0 {
			countState.FavoriteCount = 0
		}
		countState.CountVersion++
		if err := tx.Model(&countState).Updates(map[string]interface{}{
			"favorite_count": countState.FavoriteCount,
			"count_version":  countState.CountVersion,
		}).Error; err != nil {
			return err
		}
		favoriteCount = countState.FavoriteCount
		outbox := model.FavoriteCountOutbox{
			EventID:       eventID,
			UserID:        userID,
			AuthorID:      authorID,
			VideoID:       videoID,
			CountVersion:  countState.CountVersion,
			Delta:         delta,
			FavoriteCount: countState.FavoriteCount,
		}
		return tx.Create(&outbox).Error
	})
	if err != nil {
		return false, 0, err
	}
	// favorite 与 user 都归 user.rpc；Redis 缓存删除失败不回滚已提交的持久 Outbox。
	if err := cache.FavoriteAction(userID, authorID); err != nil {
		zap.L().Warn("清除点赞用户缓存失败", zap.Error(err))
	}
	return changed, favoriteCount, nil
}

func lockFavoriteVideoCountState(tx *gorm.DB, videoID uint64) (model.FavoriteVideoCountState, error) {
	var state model.FavoriteVideoCountState
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("video_id = ?", videoID).Take(&state).Error
	if err == nil {
		return state, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return state, err
	}
	var count int64
	if err := tx.Model(&model.Favorite{}).Where("video_id = ?", videoID).Count(&count).Error; err != nil {
		return state, err
	}
	initial := model.FavoriteVideoCountState{VideoID: videoID, FavoriteCount: count}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&initial).Error; err != nil {
		return state, err
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("video_id = ?", videoID).Take(&state).Error; err != nil {
		return state, err
	}
	return state, nil
}
