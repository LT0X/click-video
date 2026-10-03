package chat

import (
	"context"
	"testing"

	"douyin/rpc/chatpush"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestChatRPCAuthUnaryInterceptorRejectsMissingOrInvalidToken(t *testing.T) {
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret")
	handler := func(context.Context, interface{}) (interface{}, error) {
		return &chatpush.DeliveryResponse{Delivered: true}, nil
	}

	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "missing"},
		{name: "invalid", value: "attacker-value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(chatRPCTokenMetadataKey, test.value))
			_, err := interceptor(ctx, &chatpush.DeliveryRequest{}, &grpc.UnaryServerInfo{}, handler)
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("interceptor error = %v, want unauthenticated", err)
			}
		})
	}
}

func TestChatRPCAuthUnaryInterceptorAllowsValidToken(t *testing.T) {
	interceptor := chatRPCAuthUnaryInterceptor("expected-secret")
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(chatRPCTokenMetadataKey, "expected-secret"))
	called := false
	_, err := interceptor(ctx, &chatpush.DeliveryRequest{}, &grpc.UnaryServerInfo{}, func(context.Context, interface{}) (interface{}, error) {
		called = true
		return &chatpush.DeliveryResponse{Delivered: true}, nil
	})
	if err != nil || !called {
		t.Fatalf("interceptor err=%v, handler called=%v; want success", err, called)
	}
}
