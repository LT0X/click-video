package chat

import (
	"context"
	"errors"
	"strconv"
	"sync"
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
	target := "gateway-a.internal:8014"
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret", target, newChatRPCReplayGuard(&memoryChatRPCNonceStore{}))
	handler := func(context.Context, interface{}) (interface{}, error) {
		return &chatpush.DeliveryResponse{Delivered: true}, nil
	}

	validValues := signedRPCMetadata(t, "expected-secret", target, request, time.Now(), "0123456789abcdef0123456789abcdef0123456789abcdef")
	wrongSignature := validValues.Copy()
	wrongSignature.Set(chatRPCSignatureMetadataKey, "invalid-signature")
	staleValues := signedRPCMetadata(t, "expected-secret", target, request, time.Now().Add(-chatRPCAuthWindow-time.Second), "1123456789abcdef0123456789abcdef0123456789abcdef")

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
	target := "gateway-a.internal:8014"
	values := signedRPCMetadata(t, "expected-secret", target, request, time.Now(), "2123456789abcdef0123456789abcdef0123456789abcdef")
	ctx := metadata.NewIncomingContext(context.Background(), values)
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret", target, newChatRPCReplayGuard(&memoryChatRPCNonceStore{}))
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

func TestChatRPCAuthUnaryInterceptorBindsRequestToTargetGateway(t *testing.T) {
	request := &chatpush.DeliveryRequest{EventID: "event-3", Content: "targeted message"}
	values := signedRPCMetadata(t, "expected-secret", "gateway-a.internal:8014", request, time.Now(), "3123456789abcdef0123456789abcdef0123456789abcdef")
	ctx := metadata.NewIncomingContext(context.Background(), values)
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret", "gateway-b.internal:8014", newChatRPCReplayGuard(&memoryChatRPCNonceStore{}))
	_, err := interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: chatpush.ChatPush_Deliver_FullMethodName}, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler called for a request signed for another gateway")
		return nil, nil
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("interceptor error = %v, want unauthenticated", err)
	}
}

func TestReplayGuardRetainsNonceUntilSignatureExpires(t *testing.T) {
	now := time.Unix(100, 0)
	store := &memoryChatRPCNonceStore{}
	guard := newChatRPCReplayGuard(store)
	expiresAt := now.Add(2 * chatRPCAuthWindow)
	accepted, err := guard.accept(context.Background(), "gateway-a.internal:8014", "nonce", expiresAt, now)
	if err != nil || !accepted {
		t.Fatalf("first nonce use accepted=%v error=%v", accepted, err)
	}
	if store.lastTTL < 59*time.Second || store.lastTTL > 60*time.Second {
		t.Fatalf("stored nonce TTL = %v, want 59–60 seconds for positive clock skew", store.lastTTL)
	}
	accepted, err = guard.accept(context.Background(), "gateway-a.internal:8014", "nonce", expiresAt, now.Add(chatRPCAuthWindow+time.Second))
	if err != nil {
		t.Fatalf("second nonce check error = %v", err)
	}
	if accepted {
		t.Fatal("nonce was accepted again before the signed timestamp expired")
	}
}

func TestRPCReplayGuardRejectsNonceAcrossInstances(t *testing.T) {
	target := "gateway-a.internal:8014"
	request := &chatpush.DeliveryRequest{EventID: "event-4", Content: "restart-safe"}
	values := signedRPCMetadata(t, "expected-secret", target, request, time.Now(), "4123456789abcdef0123456789abcdef0123456789abcdef")
	ctx := metadata.NewIncomingContext(context.Background(), values)
	store := &memoryChatRPCNonceStore{}
	guards := []*chatRPCReplayGuard{newChatRPCReplayGuard(store), newChatRPCReplayGuard(store)}
	for i, guard := range guards {
		interceptor := chatRPCAuthUnaryInterceptor("expected-secret", target, guard)
		_, err := interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: chatpush.ChatPush_Deliver_FullMethodName}, func(context.Context, interface{}) (interface{}, error) {
			return &chatpush.DeliveryResponse{Delivered: true}, nil
		})
		if i == 0 && err != nil {
			t.Fatalf("first instance rejected request: %v", err)
		}
		if i == 1 && status.Code(err) != codes.Unauthenticated {
			t.Fatalf("second instance replay error = %v, want unauthenticated", err)
		}
	}
}

func TestChatRPCAuthUnaryInterceptorFailsClosedWhenNonceStoreUnavailable(t *testing.T) {
	target := "gateway-a.internal:8014"
	request := &chatpush.DeliveryRequest{EventID: "event-5", Content: "signed"}
	values := signedRPCMetadata(t, "expected-secret", target, request, time.Now(), "5123456789abcdef0123456789abcdef0123456789abcdef")
	ctx := metadata.NewIncomingContext(context.Background(), values)
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret", target, newChatRPCReplayGuard(failingChatRPCNonceStore{}))
	_, err := interceptor(ctx, request, &grpc.UnaryServerInfo{FullMethod: chatpush.ChatPush_Deliver_FullMethodName}, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("handler called while the shared replay store is unavailable")
		return nil, nil
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("interceptor error = %v, want unavailable", err)
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

func signedRPCMetadata(t *testing.T, token, target string, request *chatpush.DeliveryRequest, requestTime time.Time, nonce string) metadata.MD {
	t.Helper()
	timestamp := strconv.FormatInt(requestTime.UnixMilli(), 10)
	signature, err := chatRPCSignature(token, chatpush.ChatPush_Deliver_FullMethodName, target, timestamp, nonce, request)
	if err != nil {
		t.Fatalf("chatRPCSignature() error = %v", err)
	}
	return metadata.Pairs(
		chatRPCTargetMetadataKey, target,
		chatRPCTimestampMetadataKey, timestamp,
		chatRPCNonceMetadataKey, nonce,
		chatRPCSignatureMetadataKey, signature,
	)
}

type memoryChatRPCNonceStore struct {
	mu      sync.Mutex
	seen    map[string]time.Time
	lastTTL time.Duration
}

func (s *memoryChatRPCNonceStore) Claim(_ context.Context, key string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = make(map[string]time.Time)
	}
	s.lastTTL = ttl
	if expiry, exists := s.seen[key]; exists && expiry.After(time.Now()) {
		return false, nil
	}
	s.seen[key] = time.Now().Add(ttl)
	return true, nil
}

type failingChatRPCNonceStore struct{}

func (failingChatRPCNonceStore) Claim(context.Context, string, time.Duration) (bool, error) {
	return false, errors.New("redis unavailable")
}
