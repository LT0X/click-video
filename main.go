package main

import (
	"context"
	"douyin/config"
	"douyin/database"
	"douyin/handler"
	"douyin/package/cache"
	"douyin/package/chat"
	"douyin/package/llm"
	"douyin/package/metrics"
	"douyin/package/mq"
	"douyin/package/upload"
	"douyin/package/util"
	"douyin/package/ws"
	"douyin/router"
	"douyin/rpc/contact/contact"
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
	metrics.StartHTTPServer(config.System.Monitoring.ListenAddressOrDefault(), "gateway", metrics.Default.Handler())
	database.InitMySQL()
	database.NewRPCServiceContext()
	cache.InitRedis()
	chatRPCToken, err := chat.ResolveRPCToken(config.System.Chat.RPCToken, config.System.Chat.RPCListenAddress)
	if err != nil {
		zap.L().Fatal("聊天节点 gRPC 认证配置无效", zap.Error(err))
	}
	chatRegistry := chat.NewRegistry()
	chatRoutes := chat.NewRedisRoutes(cache.ChatRedisClient)
	chatRemote := chat.NewGRPCRemotePusher(chatRPCToken)
	chatCache := chat.NewCache(cache.ChatRedisClient)
	chatBuffer := chat.NewStreamBuffer(cache.ChatRedisClient, "", func(ctx context.Context, messages []chat.Message) error {
		request := &contact.CreateMessagesBatchRequest{Messages: make([]*contact.ChatMessageInput, 0, len(messages))}
		for _, msg := range messages {
			request.Messages = append(request.Messages, &contact.ChatMessageInput{
				EventID: msg.EventID, UserID: msg.FromUserID, ToUserID: msg.ToUserID,
				Content: msg.Content, CreateTime: msg.CreateTime,
			})
		}
		if _, err := database.RPC.ContactRpc.CreateMessagesBatch(ctx, request); err != nil {
			return err
		}
		return chatCache.AfterPersist(messages)
	})
	chatDispatcher := chat.NewDispatcher(config.System.Chat.AdvertiseAddress, chatRegistry, chatBuffer, chatRoutes, chatRemote)
	llm.ConfigureChatMessageSender(func(ctx context.Context, msg chat.Message) error {
		if err := chatDispatcher.Send(ctx, msg); err != nil {
			return err
		}
		go func() {
			if err := chatCache.OnMessageQueued(context.Background(), msg); err != nil {
				zap.L().Warn("更新 AI 聊天消息缓存失败", zap.Error(err))
			}
		}()
		return nil
	})
	service.ConfigureChatPipeline(chatDispatcher, chatCache)
	ws.ConfigureChat(chatRegistry, chatRoutes, chatDispatcher, config.System.Chat.AdvertiseAddress)
	if _, err := chat.StartPushRPC(config.System.Chat.RPCListenAddress, config.System.Chat.AdvertiseAddress, chatRegistry, chatRPCToken, cache.ChatRedisClient); err != nil {
		zap.L().Fatal("聊天节点 gRPC 推送服务启动失败", zap.Error(err))
	}
	go chatBuffer.Run(context.Background())
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
