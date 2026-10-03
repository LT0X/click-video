package chat

import (
	"context"
	"testing"

	"douyin/rpc/chatpush"
)

func TestGatewayPushServerDeliversToLocalConnection(t *testing.T) {
	registry := NewRegistry()
	socket := &fakeSocket{}
	registry.Register(17, NewClient(socket))
	server := NewGatewayPushServer(registry)
	response, err := server.Deliver(context.Background(), &chatpush.DeliveryRequest{
		EventID: "rpc-event", Content: "跨节点", CreateTime: 100,
		FromUserID: 3, ToUserID: 17,
	})
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if !response.GetDelivered() || len(socket.written) != 1 {
		t.Fatalf("response=%v deliveries=%d, want delivered once", response.GetDelivered(), len(socket.written))
	}
}

func TestGatewayPushServerReportsOfflineRecipientWithoutRPCFailure(t *testing.T) {
	server := NewGatewayPushServer(NewRegistry())
	response, err := server.Deliver(context.Background(), &chatpush.DeliveryRequest{
		EventID: "rpc-event", Content: "offline", CreateTime: 100,
		FromUserID: 3, ToUserID: 17,
	})
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if response.GetDelivered() {
		t.Fatal("offline recipient reported as delivered")
	}
}
