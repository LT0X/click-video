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
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	chatRPCTimestampMetadataKey = "x-chat-rpc-timestamp"
	chatRPCNonceMetadataKey     = "x-chat-rpc-nonce"
	chatRPCSignatureMetadataKey = "x-chat-rpc-signature"
	chatRPCAuthWindow           = 30 * time.Second
)

// chatRPCReplayGuard 限制认证请求在有效期内只能被同一个节点接受一次。
type chatRPCReplayGuard struct {
	mu        sync.Mutex
	seen      map[string]time.Time
	nextPrune time.Time
}

func newChatRPCReplayGuard() *chatRPCReplayGuard {
	return &chatRPCReplayGuard{seen: make(map[string]time.Time)}
}

func (g *chatRPCReplayGuard) accept(nonce string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if expiry, exists := g.seen[nonce]; exists && expiry.After(now) {
		return false
	}
	if !now.Before(g.nextPrune) {
		for oldNonce, expiry := range g.seen {
			if !expiry.After(now) {
				delete(g.seen, oldNonce)
			}
		}
		g.nextPrune = now.Add(chatRPCAuthWindow)
	}
	g.seen[nonce] = now.Add(chatRPCAuthWindow)
	return true
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

func chatRPCSignature(token, method, timestamp, nonce string, request proto.Message) (string, error) {
	requestBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return "", fmt.Errorf("序列化聊天节点 RPC 请求失败: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte(method))
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
func chatRPCAuthUnaryInterceptor(token string, replayGuard *chatRPCReplayGuard) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		values, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		timestampValue, timestampOK := singleMetadataValue(values, chatRPCTimestampMetadataKey)
		nonce, nonceOK := singleMetadataValue(values, chatRPCNonceMetadataKey)
		signature, signatureOK := singleMetadataValue(values, chatRPCSignatureMetadataKey)
		if !timestampOK || !nonceOK || !signatureOK || strings.TrimSpace(token) == "" || len(nonce) != 48 {
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
		if requestTime.Before(now.Add(-chatRPCAuthWindow)) || requestTime.After(now.Add(chatRPCAuthWindow)) {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 请求已过期")
		}
		protobufRequest, ok := req.(proto.Message)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 请求类型无效")
		}
		expected, err := chatRPCSignature(token, info.FullMethod, timestampValue, nonce, protobufRequest)
		if err != nil || !hmac.Equal([]byte(expected), []byte(signature)) {
			return nil, status.Error(codes.Unauthenticated, "聊天节点 RPC 认证失败")
		}
		if replayGuard == nil || !replayGuard.accept(nonce, now) {
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
