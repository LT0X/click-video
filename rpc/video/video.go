package main

import (
	"context"
	appconfig "douyin/config"
	"douyin/package/metrics"
	eventmq "douyin/package/mq"
	"douyin/package/util"
	"douyin/rpc/video/internal/cache"
	"flag"
	"fmt"

	"douyin/rpc/video/internal/config"
	"douyin/rpc/video/internal/logic"
	favoritemq "douyin/rpc/video/internal/mq"
	"douyin/rpc/video/internal/server"
	"douyin/rpc/video/internal/svc"
	"douyin/rpc/video/video"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/video.yaml", "the config file")

func main() {
	flag.Parse()
	util.InitZap()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	metrics.StartHTTPServer(c.MetricsListenAddress(), "video.rpc", metrics.Default.Handler())
	svcCtx := svc.NewServiceContext(c)
	cache.InitRedis(svcCtx)

	rabbitMQConfig, err := appconfig.LoadRabbitMQ()
	if err != nil {
		logx.Errorf("加载 RabbitMQ 配置失败: %v", err)
		return
	}
	eventBroker, err := eventmq.NewFavoriteEventBroker(rabbitMQConfig)
	if err != nil {
		logx.Errorf("连接 RabbitMQ 失败: %v", err)
		return
	}
	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer func() {
		cancelConsumers()
		_ = eventBroker.Close()
	}()
	go favoritemq.RunFavoriteCountConsumer(consumerCtx, eventBroker, svcCtx)
	go favoritemq.RunCommentCacheInvalidationOutbox(consumerCtx, eventBroker, svcCtx)
	logic.RunFavoriteCounterWorkers(consumerCtx, svcCtx)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		video.RegisterVideoServer(grpcServer, server.NewVideoServer(svcCtx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()
	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
