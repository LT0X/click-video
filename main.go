package main

import (
	"context"
	"douyin/config"
	"douyin/database"
	"douyin/handler"
	"douyin/package/cache"
	"douyin/package/llm"
	"douyin/package/mq"
	"douyin/package/upload"
	"douyin/package/util"
	"douyin/router"
	"douyin/rpc/user/user"
	"douyin/rpc/video/video"
	"douyin/service"
	"path/filepath"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"go.uber.org/zap"
)

func main() {
	// 手动调用初始化函数 可以考虑使用init函数
	config.Init()
	util.InitZap()
	database.InitMySQL()
	database.NewRPCServiceContext()
	cache.InitRedis()
	uploadConfig := upload.Config{
		TempDir:  config.System.Upload.TempDir,
		VideoDir: filepath.Join(config.System.HttpAddress.VideoAddress, "videos"),
		PartSize: config.System.Upload.PartSize, MaxChunkSize: config.System.Upload.MaxChunkSize,
		MaxUploadSize: config.System.Upload.MaxUploadSize, MaxParts: config.System.Upload.MaxParts,
		MergeConcurrency: config.System.Upload.MergeConcurrency,
	}
	uploadManager := upload.NewManager(uploadConfig)
	uploadTTL := time.Duration(config.System.Upload.TTLHours) * time.Hour
	uploadState := service.NewUploadState(cache.VideoRedisClient, uploadManager, uploadTTL, time.Duration(config.System.Upload.TTLJitterSeconds)*time.Second)
	uploadService := service.NewVideoUploadService(service.UploadServiceConfig{
		Upload: uploadConfig, CoverDir: filepath.Join(config.System.HttpAddress.VideoAddress, "covers"),
		PublicBaseURL: config.System.Upload.PublicBaseURL,
	}, uploadManager, uploadState, service.UploadRPC{
		CreateVideo: func(ctx context.Context, req *video.CreateVideoRequest) (*video.CreateVideoResponse, error) {
			return database.RPC.VideoRpc.CreateVideo(ctx, req)
		},
		IncrementWorkCount: func(ctx context.Context, req *user.IncrementWorkCountRequest) (*user.IncrementWorkCountResponse, error) {
			return database.RPC.UserRpc.IncrementWorkCount(ctx, req)
		},
		UpdateVideoURL: func(ctx context.Context, req *video.UpdateVideoURLRequest) (*video.UpdateVideoURLResponse, error) {
			return database.RPC.VideoRpc.UpdateVideoURL(ctx, req)
		},
	}, util.GetSnapshot)
	go service.RunUploadCleanup(context.Background(), uploadManager, uploadState, time.Duration(config.System.Upload.CleanupIntervalMinutes)*time.Minute, uploadTTL)

	mq.InitMQ()
	llm.RegisterChatGPT()
	// 客户端文件超过30MB 返回413
	app := fiber.New(fiber.Config{
		BodyLimit:                    30 * 1024 * 1024,
		StreamRequestBody:            true,
		DisablePreParseMultipartForm: true,
	})
	// 上传分片和合并中间文件不应通过静态文件路由下载。
	router.ProtectUploadInternalFiles(app, "/static", config.System.HttpAddress.VideoAddress, config.System.Upload.TempDir)
	// 使用中间件打印日志
	app.Static("/static", config.System.HttpAddress.VideoAddress)
	app.Use(logger.New())
	router.InitRouter(app, handler.NewUploadHandler(uploadService))
	zap.L().Fatal("fiber启动失败: ", zap.Error(app.Listen(
		config.System.HttpAddress.Host+":"+config.System.HttpAddress.Port)))
}
