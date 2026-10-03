package cache

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"douyin/package/metrics"

	"github.com/go-redis/redis"
)

func TestFavoriteCountMetricClassifiesCacheOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		value      string
		commandErr error
		wantCount  int64
		wantFound  bool
		wantResult string
	}{
		{name: "hit", value: "42", wantCount: 42, wantFound: true, wantResult: "hit"},
		{name: "miss", commandErr: redis.Nil, wantResult: "miss"},
		{name: "malformed value", value: "not-a-count", wantResult: "error"},
		{name: "redis error", commandErr: errors.New("redis unavailable"), wantResult: "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			registry := metrics.NewRegistry()
			count, found, result := recordFavoriteCountCacheAccess(registry, tc.value, tc.commandErr)
			if count != tc.wantCount || found != tc.wantFound {
				t.Fatalf("got count=%d found=%t, want count=%d found=%t", count, found, tc.wantCount, tc.wantFound)
			}
			if result != tc.wantResult {
				t.Fatalf("got result=%q, want %q", result, tc.wantResult)
			}
			response := httptest.NewRecorder()
			registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
			if response.Code != 200 {
				t.Fatalf("metrics endpoint returned HTTP %d", response.Code)
			}
			wantSample := `click_video_favorite_count_cache_access_total{result="` + tc.wantResult + `"} 1`
			if !strings.Contains(response.Body.String(), wantSample) {
				t.Errorf("metrics output is missing sample %q", wantSample)
			}
			if strings.Contains(response.Body.String(), "redis unavailable") {
				t.Fatal("Redis error detail leaked into metric labels")
			}
		})
	}
}
