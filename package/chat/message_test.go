package chat

import (
	"testing"
	"time"
)

func TestNewMessageBuildsValidatedUniqueEvents(t *testing.T) {
	first, err := NewMessage(1, 2, "first", time.UnixMilli(100))
	if err != nil {
		t.Fatalf("NewMessage() error = %v", err)
	}
	second, err := NewMessage(1, 2, "second", time.UnixMilli(101))
	if err != nil {
		t.Fatalf("NewMessage() error = %v", err)
	}
	if first.EventID == "" || first.EventID == second.EventID || first.CreateTime != 100 || first.Content != "first" {
		t.Fatalf("messages = %#v / %#v", first, second)
	}
}

func TestNewMessageRejectsMissingFields(t *testing.T) {
	for _, args := range []struct {
		from, to uint64
		content  string
	}{
		{from: 0, to: 2, content: "x"},
		{from: 1, to: 0, content: "x"},
		{from: 1, to: 2, content: " "},
	} {
		if _, err := NewMessage(args.from, args.to, args.content, time.Now()); err == nil {
			t.Fatalf("NewMessage(%#v) error = nil", args)
		}
	}
}
