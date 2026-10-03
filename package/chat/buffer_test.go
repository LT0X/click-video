package chat

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestPersistThenAcknowledgeOnlyAfterSuccessfulWrite(t *testing.T) {
	order := make([]string, 0, 2)
	messages := []Message{{EventID: "event-1", FromUserID: 1, ToUserID: 2, Content: "消息"}}
	err := PersistThenAcknowledge(context.Background(), messages,
		func(_ context.Context, got []Message) error {
			order = append(order, "persist")
			if !reflect.DeepEqual(got, messages) {
				t.Fatalf("persisted messages = %#v", got)
			}
			return nil
		}, func() error {
			order = append(order, "ack")
			return nil
		})
	if err != nil {
		t.Fatalf("PersistThenAcknowledge() error = %v", err)
	}
	if !reflect.DeepEqual(order, []string{"persist", "ack"}) {
		t.Fatalf("operation order = %#v", order)
	}
}

func TestPersistThenAcknowledgeKeepsEntriesOnWriteFailure(t *testing.T) {
	acked := false
	err := PersistThenAcknowledge(context.Background(), []Message{{EventID: "event-2"}},
		func(context.Context, []Message) error { return errors.New("contact database unavailable") },
		func() error { acked = true; return nil })
	if err == nil {
		t.Fatal("PersistThenAcknowledge() error = nil, want persistence error")
	}
	if acked {
		t.Fatal("message was acknowledged after persistence failure")
	}
}

func TestDecodeStreamMessageRejectsMalformedOrIncompletePayload(t *testing.T) {
	for _, raw := range []string{
		"{",
		`{"event_id":"x","from_user_id":1,"to_user_id":2}`,
		`{"event_id":"x","from_user_id":1,"to_user_id":2,"content":"ok"}`,
	} {
		if _, err := DecodeStreamMessage(raw); err == nil {
			t.Fatalf("DecodeStreamMessage(%q) error = nil", raw)
		}
	}
	msg, err := DecodeStreamMessage(`{"event_id":"e1","from_user_id":1,"to_user_id":2,"content":"ok","create_time":42}`)
	if err != nil {
		t.Fatalf("DecodeStreamMessage() error = %v", err)
	}
	if msg.EventID != "e1" || msg.CreateTime != 42 {
		t.Fatalf("decoded message = %#v", msg)
	}
}

func TestShouldFlushBatchAtLimitOrWindow(t *testing.T) {
	started := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	window := 250 * time.Millisecond
	if ShouldFlushBatch(49, started, started.Add(window-time.Millisecond), 50, window) {
		t.Fatal("batch flushed before reaching count or timer")
	}
	if !ShouldFlushBatch(50, started, started, 50, window) {
		t.Fatal("full batch was not flushed")
	}
	if !ShouldFlushBatch(1, started, started.Add(window), 50, window) {
		t.Fatal("timer did not flush a partial batch")
	}
}
