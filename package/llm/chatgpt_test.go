package llm

import (
	"context"
	"douyin/package/chat"
	"testing"
)

func TestQueueChatMessageUsesConfiguredBufferedSender(t *testing.T) {
	var sent chat.Message
	called := false
	msg, err := queueChatMessage(context.Background(), 8, ChatGPTID, "prompt", func(_ context.Context, queued chat.Message) error {
		called = true
		sent = queued
		return nil
	})
	if err != nil {
		t.Fatalf("queueChatMessage() error = %v", err)
	}
	if !called || msg.EventID == "" || sent.EventID != msg.EventID || sent.FromUserID != 8 || sent.ToUserID != ChatGPTID || sent.Content != "prompt" {
		t.Fatalf("returned=%#v sent=%#v called=%v", msg, sent, called)
	}
}

func TestQueueChatMessageRejectsMissingSender(t *testing.T) {
	if _, err := queueChatMessage(context.Background(), 8, ChatGPTID, "prompt", nil); err == nil {
		t.Fatal("queueChatMessage() error = nil, want unconfigured sender error")
	}
}
