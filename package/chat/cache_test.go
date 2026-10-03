package chat

import (
	"testing"
	"time"
)

func TestConversationCacheKeysAreSharedAcrossDirection(t *testing.T) {
	if HistoryKey(3, 8) != HistoryKey(8, 3) {
		t.Fatal("conversation history keys differ by direction")
	}
	if NewestKey(3, 8) == NewestKey(8, 3) {
		t.Fatal("friend newest-message keys must remain user-scoped")
	}
}

func TestHistoryCompletionNeverMarksTruncatedWindowComplete(t *testing.T) {
	if CanMarkHistoryComplete(0) {
		t.Fatal("empty history must use DB miss instead of a marker without a TTL-bound ZSet")
	}
	if !CanMarkHistoryComplete(HistoryLimit - 1) {
		t.Fatal("history shorter than cache limit should be complete")
	}
	if CanMarkHistoryComplete(HistoryLimit) || CanMarkHistoryComplete(HistoryLimit+1) {
		t.Fatal("history at or above cache limit may be truncated")
	}
}

func TestChatCacheTTLAddsBoundedJitter(t *testing.T) {
	base := time.Hour
	for i := 0; i < 100; i++ {
		got := ChatCacheTTL(base)
		if got < base || got > base+5*time.Minute {
			t.Fatalf("TTL = %s, outside [%s, %s]", got, base, base+5*time.Minute)
		}
	}
}
