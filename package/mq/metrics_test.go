package mq

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"douyin/package/metrics"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestQueueDepthInspectionReturnsFixedQueueSamples(t *testing.T) {
	inspectErr := errors.New("queue inspection failed")
	var inspected []string
	samples := inspectQueueDepths(func(queue string) (int, error) {
		inspected = append(inspected, queue)
		if queue == "comment_writer" {
			return 0, inspectErr
		}
		return 7, nil
	})
	wantQueues := []string{
		FavoriteUserQueue,
		FavoriteVideoQueue,
		CacheInvalidationQueue,
		commentQueue,
		commentRetryQueue,
		commentDeadLetterQueue,
	}
	if strings.Join(inspected, ",") != strings.Join(wantQueues, ",") {
		t.Fatalf("inspected queues %v, want %v", inspected, wantQueues)
	}
	if len(samples) != len(wantQueues) {
		t.Fatalf("got %d samples, want %d", len(samples), len(wantQueues))
	}
	for i, sample := range samples {
		if sample.Queue != wantQueues[i] {
			t.Errorf("sample %d queue=%q, want %q", i, sample.Queue, wantQueues[i])
		}
		if sample.Queue == "comment_writer" {
			if !errors.Is(sample.Err, inspectErr) {
				t.Errorf("comment_writer error=%v, want %v", sample.Err, inspectErr)
			}
			continue
		}
		if sample.Err != nil || sample.Messages != 7 {
			t.Errorf("sample %q = %+v, want 7 messages without error", sample.Queue, sample)
		}
	}
}

func TestQueueDepthInspectionUsesIndependentChannelForEveryQueue(t *testing.T) {
	channelCount := 0
	samples := inspectQueueDepthsWithChannels(func() (queueInspectionChannel, error) {
		channelCount++
		return &fakeQueueInspectionChannel{failInspection: channelCount == 1}, nil
	})

	if channelCount != len(monitoredQueueNames) {
		t.Fatalf("opened %d channels, want one channel per queue (%d)", channelCount, len(monitoredQueueNames))
	}
	if len(samples) != len(monitoredQueueNames) || samples[0].Err == nil {
		t.Fatalf("first queue should report its missing-queue error: %+v", samples)
	}
	for _, sample := range samples[1:] {
		if sample.Err != nil || sample.Messages != 3 {
			t.Errorf("queue %q was affected by another queue's channel error: %+v", sample.Queue, sample)
		}
	}
}

type fakeQueueInspectionChannel struct {
	failInspection bool
	closed         bool
}

func (channel *fakeQueueInspectionChannel) InspectQueueDepth(string) (int, error) {
	if channel.closed {
		return 0, errors.New("channel is closed")
	}
	if channel.failInspection {
		channel.closed = true
		return 0, errors.New("queue does not exist")
	}
	return 3, nil
}

func (channel *fakeQueueInspectionChannel) Close() error {
	channel.closed = true
	return nil
}

func TestQueueDepthSamplesRetainGaugeWhenInspectionFails(t *testing.T) {
	registry := metrics.NewRegistry()
	applyQueueDepthSamples(registry, []QueueDepthSample{{Queue: FavoriteUserQueue, Messages: 9}})
	applyQueueDepthSamples(registry, []QueueDepthSample{{Queue: FavoriteUserQueue, Err: errors.New("broker unavailable")}})

	body := scrapeMetrics(t, registry)
	if !strings.Contains(body, `click_video_rabbitmq_queue_messages{queue="favorite_user_rpc"} 9`) {
		t.Fatal("failed inspection overwrote the last known queue depth")
	}
	if !strings.Contains(body, `click_video_rabbitmq_queue_inspection_errors_total{queue="favorite_user_rpc"} 1`) {
		t.Fatal("failed queue inspection was not counted")
	}
}

func TestQueueDepthMonitorSamplesImmediatelyAndStopsOnContext(t *testing.T) {
	registry := metrics.NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	runQueueDepthMonitor(ctx, registry, func() []QueueDepthSample {
		calls++
		cancel()
		return []QueueDepthSample{{Queue: FavoriteUserQueue, Messages: 4}}
	}, 0)
	if calls != 1 {
		t.Fatalf("inspect called %d times, want one immediate sample", calls)
	}
	if !strings.Contains(scrapeMetrics(t, registry), `click_video_rabbitmq_queue_messages{queue="favorite_user_rpc"} 4`) {
		t.Fatal("immediate queue sample was not recorded")
	}
}

func TestQueueDepthInspectionUnavailableReturnsErrorsForFixedQueues(t *testing.T) {
	var broker *FavoriteEventBroker
	samples := broker.InspectQueueDepths()
	if len(samples) != len(monitoredQueueNames) {
		t.Fatalf("got %d queue errors, want %d", len(samples), len(monitoredQueueNames))
	}
	for _, sample := range samples {
		if sample.Err == nil {
			t.Errorf("queue %q unexpectedly has a successful sample", sample.Queue)
		}
	}
}

func TestDeliveryLagRejectsMissingAndFutureTimestamps(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		timestamp time.Time
		wantLag   time.Duration
		wantOK    bool
	}{
		{name: "missing", timestamp: time.Time{}, wantOK: false},
		{name: "future", timestamp: now.Add(time.Second), wantOK: false},
		{name: "valid", timestamp: now.Add(-1500 * time.Millisecond), wantLag: 1500 * time.Millisecond, wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lag, ok := deliveryLag(tc.timestamp, now)
			if lag != tc.wantLag || ok != tc.wantOK {
				t.Fatalf("got lag=%s ok=%t, want lag=%s ok=%t", lag, ok, tc.wantLag, tc.wantOK)
			}
		})
	}
}

func TestConsumerLagRecordsOnlyValidTimestamps(t *testing.T) {
	registry := metrics.NewRegistry()
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	recordConsumerLag(registry, FavoriteUserQueue, time.Time{}, now)
	recordConsumerLag(registry, FavoriteUserQueue, now.Add(-2*time.Second), now)
	recordConsumerLag(registry, FavoriteUserQueue, now.Add(time.Second), now)

	body := scrapeMetrics(t, registry)
	if !strings.Contains(body, `click_video_rabbitmq_consumer_messages_missing_timestamp_total{queue="favorite_user_rpc"} 1`) {
		t.Error("missing message timestamp was not counted")
	}
	if !strings.Contains(body, `click_video_rabbitmq_consumer_lag_seconds_count{queue="favorite_user_rpc"} 1`) {
		t.Error("valid consumer lag was not observed, or a future timestamp was included")
	}
}

func TestPublisherTimestampIsAddedOnlyWhenMissing(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 123, time.FixedZone("UTC+8", 8*60*60))
	var message amqp.Publishing
	recordPublishingTimestamp(&message, now)
	if !message.Timestamp.Equal(now.UTC()) {
		t.Fatalf("timestamp=%s, want %s", message.Timestamp, now.UTC())
	}
	existing := now.Add(-time.Minute)
	message.Timestamp = existing
	recordPublishingTimestamp(&message, now)
	if !message.Timestamp.Equal(existing) {
		t.Fatalf("existing timestamp changed to %s, want %s", message.Timestamp, existing)
	}
}

func scrapeMetrics(t *testing.T, registry *metrics.Registry) string {
	t.Helper()
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("metrics endpoint returned HTTP %d", response.Code)
	}
	return response.Body.String()
}
