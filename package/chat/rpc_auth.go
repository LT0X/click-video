package chat

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const chatRPCTokenMetadataKey = "x-chat-rpc-token"

// chatRPCAuthUnaryInterceptor 只允许持有集群共享密钥的网关调用节点推送 RPC。
func chatRPCAuthUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		provided := ""
		if values, ok := metadata.FromIncomingContext(ctx); ok {
			if tokens := values.Get(chatRPCTokenMetadataKey); len(tokens) == 1 {
				provided = tokens[0]
			}
		}
		if strings.TrimSpace(token) == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		return handler(ctx, req)
	}
}
