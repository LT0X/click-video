package main

import (
	"context"
	appconfig "douyin/config"
	"douyin/package/metrics"
	"douyin/package/mq"
	"douyin/package/util"
	"douyin/rpc/user/internal/cache"
	"douyin/rpc/user/internal/config"
	"douyin/rpc/user/internal/logic"
	favoritemq "douyin/rpc/user/internal/mq"
	"douyin/rpc/user/internal/server"
	"douyin/rpc/user/internal/svc"
	"douyin/rpc/user/user"
	"flag"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/conf"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/service"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var configFile = flag.String("f", "etc/user.yaml", "the config file")

func main() {
	flag.Parse()
	util.InitZap()

	var c config.Config
	conf.MustLoad(*configFile, &c)
	metrics.StartHTTPServer(c.MetricsListenAddress(), "user.rpc", metrics.Default.Handler())
	svcCtx := svc.NewServiceContext(c)
	cache.InitRedis(svcCtx)

	rabbitMQConfig, err := appconfig.LoadRabbitMQ()
	if err != nil {
		logx.Errorf("加载 RabbitMQ 配置失败: %v", err)
		return
	}
	eventBroker, err := mq.NewFavoriteEventBroker(rabbitMQConfig)
	if err != nil {
		logx.Errorf("连接 RabbitMQ 失败: %v", err)
		return
	}
	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer func() {
		cancelConsumers()
		_ = eventBroker.Close()
	}()
	go favoritemq.RunFavoriteActionConsumer(consumerCtx, eventBroker, svcCtx)
	go logic.RunFavoriteCountOutboxPublisher(consumerCtx, svcCtx, eventBroker)
	go logic.RunFavoriteCountReconciliation(consumerCtx, svcCtx, eventBroker, 30*time.Minute)

	s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
		user.RegisterUserServer(grpcServer, server.NewUserServer(svcCtx))

		if c.Mode == service.DevMode || c.Mode == service.TestMode {
			reflection.Register(grpcServer)
		}
	})
	defer s.Stop()

	fmt.Printf("Starting rpc server at %s...\n", c.ListenOn)
	s.Start()
}
