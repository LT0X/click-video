package chat

import (
	"context"
	"strconv"
	"testing"
	"time"

	"douyin/rpc/chatpush"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestChatRPCAuthUnaryInterceptorRejectsInvalidRequests(t *testing.T) {
	request := &chatpush.DeliveryRequest{EventID: "event-1", Content: "message"}
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret", newChatRPCReplayGuard())
	handler := func(context.Context, interface{}) (interface{}, error) {
		return &chatpush.DeliveryResponse{Delivered: true}, nil
	}

	validValues := signedRPCMetadata(t, "expected-secret", request, time.Now(), "0123456789abcdef0123456789abcdef0123456789abcdef")
	wrongSignature := validValues.Copy()
	wrongSignature.Set(chatRPCSignatureMetadataKey, "invalid-signature")
	staleValues := signedRPCMetadata(t, "expected-secret", request, time.Now().Add(-chatRPCAuthWindow-time.Second), "1123456789abcdef0123456789abcdef0123456789abcdef")

	for _, test := range []struct {
		name   string
		values metadata.MD
	}{
		{name: "missing metadata", values: metadata.MD{}},
		{name: "wrong signature", values: wrongSignature},
		{name: "expired timestamp", values: staleValues},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), test.values)
			_, err := interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: chatpush.ChatPush_Deliver_FullMethodName}, handler)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("interceptor error = %v, want unauthenticated", err)
			}
		})
	}
}

func TestChatRPCAuthUnaryInterceptorAllowsSignedRequestOnce(t *testing.T) {
	request := &chatpush.DeliveryRequest{EventID: "event-2", Content: "signed message"}
	values := signedRPCMetadata(t, "expected-secret", request, time.Now(), "2123456789abcdef0123456789abcdef0123456789abcdef")
	ctx := metadata.NewIncomingContext(context.Background(), values)
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret", newChatRPCReplayGuard())
	called := 0
	handler := func(context.Context, interface{}) (interface{}, error) {
		called++
		return &chatpush.DeliveryResponse{Delivered: true}, nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: chatpush.ChatPush_Deliver_FullMethodName}
	if _, err := interceptor(ctx, request, info, handler); err != nil {
		t.Fatalf("first signed request failed: %v", err)
	}
	if _, err := interceptor(ctx, request, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("replayed request error = %v, want unauthenticated", err)
	}
	if called != 1 {
		t.Fatalf("handler calls = %d, want 1", called)
	}
}

func TestResolveRPCTokenRequiresSecretForNonLoopbackListener(t *testing.T) {
	if _, err := ResolveRPCToken("", "0.0.0.0:8014"); err == nil {
		t.Fatal("ResolveRPCToken() error = nil for non-loopback listener without configured secret")
	}
	token, err := ResolveRPCToken("configured-secret", "10.0.0.7:8014")
	if err != nil || token != "configured-secret" {
		t.Fatalf("ResolveRPCToken() = %q, %v; want configured secret", token, err)
	}
}

func TestResolveRPCTokenGeneratesEphemeralSecretForLoopback(t *testing.T) {
	first, err := ResolveRPCToken("", "127.0.0.1:8014")
	if err != nil {
		t.Fatalf("ResolveRPCToken() error = %v", err)
	}
	second, err := ResolveRPCToken("", "localhost:8014")
	if err != nil {
		t.Fatalf("second ResolveRPCToken() error = %v", err)
	}
	if first == "" || first == second {
		t.Fatal("loopback listeners should receive distinct non-empty ephemeral secrets")
	}
}

func signedRPCMetadata(t *testing.T, token string, request *chatpush.DeliveryRequest, requestTime time.Time, nonce string) metadata.MD {
	t.Helper()
	timestamp := strconv.FormatInt(requestTime.UnixMilli(), 10)
	signature, err := chatRPCSignature(token, chatpush.ChatPush_Deliver_FullMethodName, timestamp, nonce, request)
	if err != nil {
		t.Fatalf("chatRPCSignature() error = %v", err)
	}
	return metadata.Pairs(
		chatRPCTimestampMetadataKey, timestamp,
		chatRPCNonceMetadataKey, nonce,
		chatRPCSignatureMetadataKey, signature,
	)
}
