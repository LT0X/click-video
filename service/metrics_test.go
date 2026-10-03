package service

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"douyin/package/metrics"
)

func TestFavoriteMetricsClassifyActionsAndErrors(t *testing.T) {
	registry := metrics.NewRegistry()
	recordFavoriteRequestMetrics(registry, "favorite", nil, 80*time.Millisecond)
	recordFavoriteRequestMetrics(registry, "unfavorite", errors.New("database details"), 120*time.Millisecond)

	body := metricsBody(t, registry)
	for _, sample := range []string{
		`click_video_favorite_requests_total{action="favorite",result="success"} 1`,
		`click_video_favorite_requests_total{action="unfavorite",result="error"} 1`,
		`click_video_favorite_request_duration_seconds_count{action="favorite"} 1`,
		`click_video_favorite_request_duration_seconds_count{action="unfavorite"} 1`,
	} {
		if !strings.Contains(body, sample) {
			t.Errorf("metrics output is missing sample %q", sample)
		}
	}
	if strings.Contains(body, "database details") {
		t.Fatal("error detail leaked into metric labels")
	}
}

func TestChatHistoryMetricsCacheOutcomesAndFallback(t *testing.T) {
	cases := []struct {
		name        string
		cacheResult string
		source      string
		requestErr  error
		wantResult  string
	}{
		{name: "cache hit", cacheResult: "hit", source: "cache", wantResult: "success"},
		{name: "database fallback after miss", cacheResult: "miss", source: "database", wantResult: "success"},
		{name: "database error after cache error", cacheResult: "error", source: "database", requestErr: errors.New("contact rpc details"), wantResult: "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := metrics.NewRegistry()
			recordChatHistoryMetrics(registry, tc.source, tc.cacheResult, tc.requestErr, 90*time.Millisecond)
			body := metricsBody(t, registry)
			requestSample := `click_video_chat_history_requests_total{result="` + tc.wantResult + `",source="` + tc.source + `"} 1`
			cacheSample := `click_video_chat_history_cache_access_total{result="` + tc.cacheResult + `"} 1`
			if !strings.Contains(body, requestSample) {
				t.Errorf("metrics output is missing sample %q", requestSample)
			}
			if !strings.Contains(body, cacheSample) {
				t.Errorf("metrics output is missing sample %q", cacheSample)
			}
			if !strings.Contains(body, "click_video_chat_history_request_duration_seconds_count 1") {
				t.Error("chat history request duration was not observed")
			}
			if tc.cacheResult == "miss" && strings.Contains(body, `click_video_chat_history_cache_access_total{result="hit"}`) {
				t.Error("database fallback after a cache miss was incorrectly counted as a cache hit")
			}
			if strings.Contains(body, "contact rpc details") {
				t.Fatal("error detail leaked into metric labels")
			}
		})
	}
}

func metricsBody(t *testing.T, registry *metrics.Registry) string {
	t.Helper()
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 {
		t.Fatalf("metrics endpoint returned HTTP %d", response.Code)
	}
	return response.Body.String()
}
