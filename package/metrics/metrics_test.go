package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRegistryExposesAllFamiliesAndBoundsLabels(t *testing.T) {
	registry := NewRegistry()
	registry.ObserveFavoriteRequest("favorite", "success", 120*time.Millisecond)
	registry.ObserveFavoriteRequest("unfavorite", "error", 240*time.Millisecond)
	registry.ObserveFavoriteRequest("raw-user-17", "database-password", time.Second)
	registry.ObserveFavoriteCountLookup(75 * time.Millisecond)
	registry.ObserveFavoriteCountCacheAccess("hit")
	registry.ObserveFavoriteCountCacheAccess("raw-video-42")
	registry.ObserveChatHistoryRequest("cache", "success", 90*time.Millisecond)
	registry.ObserveChatHistoryRequest("contact.rpc", "database-password", time.Second)
	registry.ObserveChatHistoryCacheAccess("miss")
	registry.ObserveChatHistoryCacheAccess("raw-user-17")
	registry.SetRabbitMQQueueDepth("favorite_user_rpc", 22)
	registry.SetRabbitMQQueueDepth("raw-user-17", 999)
	registry.ObserveRabbitMQQueueInspectionError("favorite_user_rpc")
	registry.ObserveRabbitMQQueueInspectionError("raw-user-17")
	registry.ObserveRabbitMQConsumerLag("favorite_user_rpc", 1500*time.Millisecond)
	registry.ObserveRabbitMQMissingTimestamp("favorite_user_rpc")

	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("metrics endpoint returned HTTP %d", response.Code)
	}
	body := response.Body.String()

	wantFamilies := []string{
		"click_video_favorite_requests_total",
		"click_video_favorite_request_duration_seconds",
		"click_video_favorite_count_lookup_duration_seconds",
		"click_video_favorite_count_cache_access_total",
		"click_video_chat_history_requests_total",
		"click_video_chat_history_request_duration_seconds",
		"click_video_chat_history_cache_access_total",
		"click_video_rabbitmq_queue_messages",
		"click_video_rabbitmq_queue_inspection_errors_total",
		"click_video_rabbitmq_consumer_lag_seconds",
		"click_video_rabbitmq_consumer_messages_missing_timestamp_total",
	}
	for _, name := range wantFamilies {
		if !strings.Contains(body, "# HELP "+name+" ") {
			t.Errorf("metric family %q is missing", name)
		}
	}

	wantSamples := []string{
		`click_video_favorite_requests_total{action="favorite",result="success"} 1`,
		`click_video_favorite_requests_total{action="unfavorite",result="error"} 1`,
		`click_video_favorite_request_duration_seconds_count{action="favorite"} 1`,
		`click_video_favorite_count_lookup_duration_seconds_count 1`,
		`click_video_favorite_count_cache_access_total{result="hit"} 1`,
		`click_video_chat_history_requests_total{result="success",source="cache"} 1`,
		`click_video_chat_history_request_duration_seconds_count 2`,
		`click_video_chat_history_cache_access_total{result="miss"} 1`,
		`click_video_rabbitmq_queue_messages{queue="favorite_user_rpc"} 22`,
		`click_video_rabbitmq_queue_inspection_errors_total{queue="favorite_user_rpc"} 1`,
		`click_video_rabbitmq_consumer_lag_seconds_count{queue="favorite_user_rpc"} 1`,
		`click_video_rabbitmq_consumer_messages_missing_timestamp_total{queue="favorite_user_rpc"} 1`,
	}
	for _, sample := range wantSamples {
		if !strings.Contains(body, sample) {
			t.Errorf("metrics output is missing sample %q", sample)
		}
	}
	for _, unboundedValue := range []string{"raw-user-17", "raw-video-42", "database-password", "contact.rpc"} {
		if strings.Contains(body, unboundedValue) {
			t.Errorf("unbounded label value %q leaked into metrics", unboundedValue)
		}
	}
}
