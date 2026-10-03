package logic

import (
	"testing"

	"douyin/package/mq"
	"douyin/rpc/user/internal/model"
)

func TestFavoriteCountOutboxEventPreservesVersionAndAbsoluteCount(t *testing.T) {
	row := model.FavoriteCountOutbox{
		EventID: 17, VideoID: 23, CountVersion: 9, Delta: -1, FavoriteCount: 4,
	}

	got := favoriteCountOutboxEvent(row)
	want := mq.FavoriteCountEvent{
		EventID: 17, Type: mq.FavoriteCountDelta, VideoID: 23,
		Version: 9, Delta: -1, Count: 4,
	}
	if got != want {
		t.Fatalf("outbox event = %#v, want %#v", got, want)
	}
}
