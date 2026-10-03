package ws

import "testing"

func TestParseClientMessageSupportsHeartbeatAndJSONSend(t *testing.T) {
	ping, err := parseClientMessage([]byte("ping"), 0)
	if err != nil || ping.Type != "ping" {
		t.Fatalf("ping = %#v, err = %v", ping, err)
	}
	post, err := parseClientMessage([]byte(`{"type":"message","to_user_id":12,"content":"hello"}`), 0)
	if err != nil || post.Type != "message" || post.ToUserID != 12 || post.Content != "hello" {
		t.Fatalf("JSON post = %#v, err = %v", post, err)
	}
}

func TestParseClientMessagePreservesLegacyPostAndEmbeddedNewlines(t *testing.T) {
	post, err := parseClientMessage([]byte("first line\nsecond line\npost"), 12)
	if err != nil || post.Type != "message" || post.ToUserID != 12 || post.Content != "first line\nsecond line" {
		t.Fatalf("legacy post = %#v, err = %v", post, err)
	}
}

func TestParseClientMessageRejectsInvalidPayload(t *testing.T) {
	for _, raw := range []string{"unknown", `{"type":"message","content":"missing recipient"}`, `{"type":"message","to_user_id":2}`} {
		if _, err := parseClientMessage([]byte(raw), 0); err == nil {
			t.Fatalf("parseClientMessage(%q) error = nil", raw)
		}
	}
}
