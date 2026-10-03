package mq

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"douyin/model"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCacheInvalidationEventValidatesPhases(t *testing.T) {
	for _, event := range []CacheInvalidationEvent{
		{EventID: 1, VideoID: 2, Phase: CacheInvalidationImmediate},
		{EventID: 3, VideoID: 4, Phase: CacheInvalidationDelayed},
	} {
		if err := event.Validate(); err != nil {
			t.Fatalf("valid event %+v rejected: %v", event, err)
		}
	}
	if err := (CacheInvalidationEvent{EventID: 0, VideoID: 2}).Validate(); err == nil {
		t.Fatal("event without event ID should be rejected")
	}
	if err := (CacheInvalidationEvent{EventID: 1, VideoID: 0}).Validate(); err == nil {
		t.Fatal("event without video ID should be rejected")
	}
	if err := (CacheInvalidationEvent{EventID: 1, VideoID: 2, Phase: 2}).Validate(); err == nil {
		t.Fatal("unknown invalidation phase should be rejected")
	}
}

func TestProcessCacheInvalidationEventDeletesTwice(t *testing.T) {
	deleted := make([]uint64, 0, 2)
	published := make([]CacheInvalidationEvent, 0, 1)
	invalidate := func(videoID uint64) error {
		deleted = append(deleted, videoID)
		return nil
	}
	publish := func(event CacheInvalidationEvent) error {
		published = append(published, event)
		return nil
	}
	immediate := CacheInvalidationEvent{EventID: 10, VideoID: 20, Phase: CacheInvalidationImmediate}
	if err := ProcessCacheInvalidationEvent(immediate, invalidate, publish); err != nil {
		t.Fatalf("process immediate invalidation: %v", err)
	}
	if len(published) != 1 || published[0].Phase != CacheInvalidationDelayed || published[0].EventID != immediate.EventID {
		t.Fatalf("delayed event = %+v, want same event in delayed phase", published)
	}
	if err := ProcessCacheInvalidationEvent(published[0], invalidate, publish); err != nil {
		t.Fatalf("process delayed invalidation: %v", err)
	}
	if len(deleted) != 2 || deleted[0] != immediate.VideoID || deleted[1] != immediate.VideoID {
		t.Fatalf("invalidations = %v, want video %d deleted twice", deleted, immediate.VideoID)
	}
	if len(published) != 1 {
		t.Fatalf("delayed phase published another event: %+v", published)
	}
}

func TestCommentPayloadProcessorContinuesAfterPoisonMessage(t *testing.T) {
	valid := []byte(`{"id":10,"video_id":20,"user_id":30,"content":"ok","created_time":"2026-10-03T00:00:00Z"}`)
	processed := 0
	err := processCommentPayloads([][]byte{[]byte("not-json"), valid}, func(commentID, videoID uint64) error {
		processed++
		if commentID != 10 || videoID != 20 {
			t.Fatalf("processed IDs = %d/%d, want 10/20", commentID, videoID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("processor error = %v", err)
	}
	if processed != 1 {
		t.Fatalf("valid payloads processed = %d, want 1 after poison payload", processed)
	}
}

func TestDecodeCommentPayloadRejectsOverlongContent(t *testing.T) {
	payload, err := json.Marshal(model.Comment{
		ID: 1, VideoID: 2, UserID: 3, Content: strings.Repeat("字", 256), CreatedTime: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCommentPayload(payload); err == nil {
		t.Fatal("overlong comment payload should be rejected as poison")
	}
}

func TestCommentRetryCountReadsMainQueueRejections(t *testing.T) {
	headers := amqp.Table{"x-death": []interface{}{
		amqp.Table{"queue": commentRetryQueue, "reason": "expired", "count": int64(3)},
		amqp.Table{"queue": commentQueue, "reason": "rejected", "count": int64(2)},
	}}
	if got := commentRetryCount(headers); got != 2 {
		t.Fatalf("comment retry count = %d, want 2", got)
	}
}

func TestPermanentCommentErrorRecognizesWrappedRPCStatus(t *testing.T) {
	err := fmt.Errorf("comment write: %w", status.Error(codes.NotFound, "video missing"))
	if !isPermanentCommentError(err) {
		t.Fatal("wrapped NotFound error should not be retried")
	}
	if isPermanentCommentError(errors.New("temporary database error")) {
		t.Fatal("ordinary errors should remain retryable")
	}
}
