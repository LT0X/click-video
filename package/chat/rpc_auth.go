package chat

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	chatRPCTimestampMetadataKey = "x-chat-rpc-timestamp"
	chatRPCTargetMetadataKey    = "x-chat-rpc-target"
	chatRPCNonceMetadataKey     = "x-chat-rpc-nonce"
	chatRPCSignatureMetadataKey = "x-chat-rpc-signature"
	chatRPCAuthWindow           = 30 * time.Second
)

type chatRPCNonceStore interface {
	Claim(context.Context, string, time.Duration) (bool, error)
}

type redisChatRPCNonceStore struct {
	client *redis.Client
}

func (s redisChatRPCNonceStore) Claim(_ context.Context, key string, ttl time.Duration) (bool, error) {
	if s.client == nil {
		return false, fmt.Errorf("聊天 RPC nonce Redis 客户端未初始化")
	}
	return s.client.SetNX(key, "1", ttl).Result()
}

// chatRPCReplayGuard 使用共享 Redis 原子 SETNX 去重，进程重启或跨节点也不会重复接受 nonce。
type chatRPCReplayGuard struct {
	store chatRPCNonceStore
}

func newChatRPCReplayGuard(store chatRPCNonceStore) *chatRPCReplayGuard {
	return &chatRPCReplayGuard{store: store}
}

func (g *chatRPCReplayGuard) accept(ctx context.Context, target, nonce string, expiresAt, now time.Time) (bool, error) {
	ttl := expiresAt.Sub(now)
	if ttl <= 0 || g.store == nil {
		return false, nil
	}
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	key := chatRPCNonceKey(target, nonce)
	return g.store.Claim(ctx, key, ttl)
}

func chatRPCNonceKey(target, nonce string) string {
	return fmt.Sprintf("chat:rpc:nonce:%s:%s", target, nonce)
}

// ResolveRPCToken 单机 loopback 开发自动生成进程内密钥，多节点监听必须显式配置共享密钥。
func ResolveRPCToken(configuredToken, listenAddress string) (string, error) {
	if strings.TrimSpace(configuredToken) != "" {
		return configuredToken, nil
	}
	host, _, err := net.SplitHostPort(listenAddress)
	if err != nil {
		return "", fmt.Errorf("解析聊天节点 RPC 监听地址 %q 失败: %w", listenAddress, err)
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return "", fmt.Errorf("多节点聊天 RPC 必须配置 CLICK_VIDEO_CHAT_RPC_TOKEN")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", fmt.Errorf("生成聊天节点 RPC 临时密钥失败: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(secret), nil
}

func chatRPCSignature(token, method, target, timestamp, nonce string, request proto.Message) (string, error) {
	requestBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return "", fmt.Errorf("序列化聊天节点 RPC 请求失败: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte(method))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(target))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(timestamp))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(nonce))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write(requestBytes)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func newChatRPCNonce() (string, error) {
	bytes := make([]byte, 24)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("生成聊天节点 RPC 随机数失败: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// chatRPCAuthUnaryInterceptor 使用共享密钥验证请求签名，避免在线路上发送密钥本身。
func chatRPCAuthUnaryInterceptor(token, target string, replayGuard *chatRPCReplayGuard) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		values, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		targetValue, targetOK := singleMetadataValue(values, chatRPCTargetMetadataKey)
		timestampValue, timestampOK := singleMetadataValue(values, chatRPCTimestampMetadataKey)
		nonce, nonceOK := singleMetadataValue(values, chatRPCNonceMetadataKey)
		signature, signatureOK := singleMetadataValue(values, chatRPCSignatureMetadataKey)
		if !targetOK || targetValue != target || !timestampOK || !nonceOK || !signatureOK || strings.TrimSpace(token) == "" || len(nonce) != 48 {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		nonceBytes, err := hex.DecodeString(nonce)
		if err != nil || len(nonceBytes) != 24 {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		timestampMillis, err := strconv.ParseInt(timestampValue, 10, 64)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		now := time.Now()
		requestTime := time.UnixMilli(timestampMillis)
		expiresAt := requestTime.Add(chatRPCAuthWindow)
		if requestTime.Before(now.Add(-chatRPCAuthWindow)) || requestTime.After(now.Add(chatRPCAuthWindow)) || !now.Before(expiresAt) {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 请求已过期")
		}
		protobufRequest, ok := req.(proto.Message)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 请求类型无效")
		}
		expected, err := chatRPCSignature(token, info.FullMethod, targetValue, timestampValue, nonce, protobufRequest)
		if err != nil || !hmac.Equal([]byte(expected), []byte(signature)) {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		if replayGuard == nil {
			return nil, status.Error(codes.Unavailable, "聊天节点 RPC 重放校验不可用")
		}
		accepted, err := replayGuard.accept(ctx, targetValue, nonce, expiresAt, now)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "聊天节点 RPC 重放校验暂不可用")
		}
		if !accepted {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 请求重复")
		}
		return handler(ctx, req)
	}
}

func singleMetadataValue(values metadata.MD, key string) (string, bool) {
	items := values.Get(key)
	if len(items) != 1 || items[0] == "" {
		return "", false
	}
	return items[0], true
}
