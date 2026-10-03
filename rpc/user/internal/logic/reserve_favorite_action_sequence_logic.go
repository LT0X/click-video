package logic

import (
	"context"
	"errors"

	"douyin/package/constant"
	"douyin/rpc/user/internal/model"
	"douyin/rpc/user/internal/svc"
	"douyin/rpc/user/user"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ReserveFavoriteActionSequence 在 user.rpc 的持久状态行中分配动作顺序，Redis 丢失不影响顺序。
func ReserveFavoriteActionSequence(ctx context.Context, svcCtx *svc.ServiceContext, userID, videoID uint64) (uint64, error) {
	if userID == 0 || videoID == 0 {
		return 0, errors.New(constant.BadParaRequest)
	}
	var sequence uint64
	err := svcCtx.DBList.Mysql.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		state := model.FavoriteActionState{UserID: userID, VideoID: videoID}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&state).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND video_id = ?", userID, videoID).Take(&state).Error; err != nil {
			return err
		}
		sequence = state.LastIssuedSequence + 1
		if sequence == 0 {
			return errors.New("点赞动作序号溢出")
		}
		return tx.Model(&state).UpdateColumn("last_issued_sequence", sequence).Error
	})
	if err != nil {
		return 0, err
	}
	return sequence, nil
}

type ReserveFavoriteActionSequenceLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewReserveFavoriteActionSequenceLogic(ctx context.Context, svcCtx *svc.ServiceContext) *ReserveFavoriteActionSequenceLogic {
	return &ReserveFavoriteActionSequenceLogic{ctx: ctx, svcCtx: svcCtx}
}

func (l *ReserveFavoriteActionSequenceLogic) ReserveFavoriteActionSequence(in *user.ReserveFavoriteActionSequenceRequest) (*user.ReserveFavoriteActionSequenceResponse, error) {
	sequence, err := ReserveFavoriteActionSequence(l.ctx, l.svcCtx, in.UserID, in.VideoID)
	if err != nil {
		return nil, err
	}
	return &user.ReserveFavoriteActionSequenceResponse{Sequence: sequence}, nil
}
