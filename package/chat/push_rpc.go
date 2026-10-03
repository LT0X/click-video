package chat

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"douyin/rpc/chatpush"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type GatewayPushServer struct {
	chatpush.UnimplementedChatPushServer
	registry *Registry
}

func NewGatewayPushServer(registry *Registry) *GatewayPushServer {
	return &GatewayPushServer{registry: registry}
}

func (s *GatewayPushServer) Deliver(_ context.Context, in *chatpush.DeliveryRequest) (*chatpush.DeliveryResponse, error) {
	msg := Message{
		EventID: in.EventID, ID: in.MessageID, Content: in.Content, CreateTime: in.CreateTime,
		FromUserID: in.FromUserID, ToUserID: in.ToUserID,
	}
	if err := validateMessage(msg); err != nil {
		return nil, err
	}
	if err := s.registry.Deliver(msg.ToUserID, msg); err != nil {
		return &chatpush.DeliveryResponse{Delivered: false}, nil
	}
	return &chatpush.DeliveryResponse{Delivered: true}, nil
}

type GRPCRemotePusher struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	token string
}

func NewGRPCRemotePusher(token string) *GRPCRemotePusher {
	return &GRPCRemotePusher{conns: make(map[string]*grpc.ClientConn), token: token}
}

func (p *GRPCRemotePusher) Push(ctx context.Context, address string, msg Message) error {
	if strings.TrimSpace(p.token) == "" {
		return fmt.Errorf("聊天节点 RPC 认证密钥未配置")
	}
	conn, err := p.connection(ctx, address)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	request := &chatpush.DeliveryRequest{
		EventID: msg.EventID, MessageID: msg.ID, Content: msg.Content, CreateTime: msg.CreateTime,
		FromUserID: msg.FromUserID, ToUserID: msg.ToUserID,
	}
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce, err := newChatRPCNonce()
	if err != nil {
		return err
	}
	signature, err := chatRPCSignature(p.token, chatpush.ChatPush_Deliver_FullMethodName, address, timestamp, nonce, request)
	if err != nil {
		return err
	}
	ctx = metadata.AppendToOutgoingContext(ctx,
		chatRPCTargetMetadataKey, address,
		chatRPCTimestampMetadataKey, timestamp,
		chatRPCNonceMetadataKey, nonce,
		chatRPCSignatureMetadataKey, signature,
	)
	_, err = chatpush.NewChatPushClient(conn).Deliver(ctx, request)
	if err != nil {
		return fmt.Errorf("跨节点推送消息失败: %w", err)
	}
	return nil
}

func (p *GRPCRemotePusher) connection(ctx context.Context, address string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn := p.conns[address]; conn != nil {
		return conn, nil
	}
	dialCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("连接聊天网关节点 %q 失败: %w", address, err)
	}
	p.conns[address] = conn
	return conn, nil
}

func (p *GRPCRemotePusher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for address, conn := range p.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("关闭聊天节点 %q 的 gRPC 连接失败: %w", address, err)
		}
		delete(p.conns, address)
	}
	return firstErr
}

func StartPushRPC(address, target string, registry *Registry, token string) (func(), error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("聊天节点 RPC 认证密钥未配置")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("监听聊天节点 RPC 地址 %q 失败: %w", address, err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(chatRPCAuthUnaryInterceptor(token, target, newChatRPCReplayGuard())))
	chatpush.RegisterChatPushServer(server, NewGatewayPushServer(registry))
	go func() {
		if err := server.Serve(listener); err != nil {
			zap.L().Error("聊天节点 gRPC 投递服务停止", zap.Error(err))
		}
	}()
	return server.Stop, nil
}
