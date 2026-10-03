package logic

import (
	"context"
	"douyin/package/constant"
	"douyin/rpc/video/internal/model"
	"gorm.io/plugin/dbresolver"

	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetCommentsByVideoIDFromMasterLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetCommentsByVideoIDFromMasterLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetCommentsByVideoIDFromMasterLogic {
	return &GetCommentsByVideoIDFromMasterLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetCommentsByVideoIDFromMasterLogic) GetCommentsByVideoIDFromMaster(in *video.GetCommentsByVideoIDFromMasterRequest) (*video.GetCommentsByVideoIDFromMasterResponse, error) {
	// todo: add your logic here and delete this line
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "视频 ID 不能为空")
	}
	exists, err := videoExists(l.ctx, l.svcCtx, in.VideoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, status.Error(codes.NotFound, constant.BloomFilterRejected)
	}
	videos := make([]*model.Comment, 0)
	err = l.svcCtx.DBList.Mysql.Clauses(dbresolver.Write).
		Model(&model.Comment{}).Where("video_id = ?", in.VideoID).
		Order("created_time desc").Find(&videos).Error
	if err != nil {
		return nil, err
	}
	return &video.GetCommentsByVideoIDFromMasterResponse{
		Comment: model.TransformRPCComments(videos),
	}, nil
}
