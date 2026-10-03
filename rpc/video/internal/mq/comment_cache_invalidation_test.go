package mq

import (
	"errors"
	"reflect"
	"testing"

	eventmq "douyin/package/mq"
	"douyin/rpc/video/internal/model"
)

func TestProcessCommentCacheInvalidationOutboxBatchRetriesPublishFailure(t *testing.T) {
	pending := []model.CommentCacheInvalidationOutbox{{ID: 1, VideoID: 10}, {ID: 2, VideoID: 20}}
	var published []eventmq.CacheInvalidationEvent
	var removed []uint64
	wantRetry := errors.New("broker unavailable")
	err := processCommentCacheInvalidationOutboxBatch(pending, func(event eventmq.CacheInvalidationEvent) error {
		if event.EventID == 1 {
			return wantRetry
		}
		published = append(published, event)
		return nil
	}, func(id uint64) error {
		removed = append(removed, id)
		return nil
	})
	if !errors.Is(err, wantRetry) {
		t.Fatalf("batch error = %v, want publish failure", err)
	}
	if !reflect.DeepEqual(published, []eventmq.CacheInvalidationEvent{{EventID: 2, VideoID: 20}}) {
		t.Fatalf("published = %+v", published)
	}
	if !reflect.DeepEqual(removed, []uint64{2}) {
		t.Fatalf("removed outbox IDs = %v", removed)
	}
}
