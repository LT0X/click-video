package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry 聚合单个服务进程的低基数运行指标。
type Registry struct {
	registry                      *prometheus.Registry
	favoriteRequests              *prometheus.CounterVec
	favoriteRequestDuration       *prometheus.HistogramVec
	favoriteCountLookupDuration   prometheus.Histogram
	favoriteCountCacheAccess      *prometheus.CounterVec
	chatHistoryRequests           *prometheus.CounterVec
	chatHistoryRequestDuration    prometheus.Histogram
	chatHistoryCacheAccess        *prometheus.CounterVec
	rabbitMQQueueMessages         *prometheus.GaugeVec
	rabbitMQQueueInspectionErrors *prometheus.CounterVec
	rabbitMQConsumerLag           *prometheus.HistogramVec
	rabbitMQMissingTimestamp      *prometheus.CounterVec
}

// Default 是当前服务进程内的默认注册表；每个微服务进程拥有独立指标数据。
var Default = NewRegistry()

// NewRegistry 创建隔离的注册表，避免测试和多个服务实例共用全局默认注册表。
func NewRegistry() *Registry {
	registry := prometheus.NewRegistry()
	metrics := &Registry{
		registry: registry,
		favoriteRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "click_video_favorite_requests_total",
			Help: "点赞和取消点赞请求总数。",
		}, []string{"action", "result"}),
		favoriteRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "click_video_favorite_request_duration_seconds",
			Help: "点赞和取消点赞请求耗时（秒）。",
		}, []string{"action"}),
		favoriteCountLookupDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "click_video_favorite_count_lookup_duration_seconds",
			Help: "video.rpc 批量读取点赞计数缓存的耗时（秒）。",
		}),
		favoriteCountCacheAccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "click_video_favorite_count_cache_access_total",
			Help: "点赞计数缓存字段访问结果总数。",
		}, []string{"result"}),
		chatHistoryRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "click_video_chat_history_requests_total",
			Help: "聊天历史查询结果总数。",
		}, []string{"source", "result"}),
		chatHistoryRequestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "click_video_chat_history_request_duration_seconds",
			Help: "聊天历史端到端查询耗时（秒）。",
		}),
		chatHistoryCacheAccess: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "click_video_chat_history_cache_access_total",
			Help: "聊天历史缓存访问结果总数。",
		}, []string{"result"}),
		rabbitMQQueueMessages: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "click_video_rabbitmq_queue_messages",
			Help: "RabbitMQ 队列最近一次采样的待处理消息数。",
		}, []string{"queue"}),
		rabbitMQQueueInspectionErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "click_video_rabbitmq_queue_inspection_errors_total",
			Help: "RabbitMQ 队列长度采样失败次数。",
		}, []string{"queue"}),
		rabbitMQConsumerLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "click_video_rabbitmq_consumer_lag_seconds",
			Help: "RabbitMQ 消息从发布到开始消费的延迟（秒）。",
		}, []string{"queue"}),
		rabbitMQMissingTimestamp: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "click_video_rabbitmq_consumer_messages_missing_timestamp_total",
			Help: "缺少发布时间戳、无法计算消费延迟的 RabbitMQ 消息数。",
		}, []string{"queue"}),
	}
	registry.MustRegister(
		metrics.favoriteRequests,
		metrics.favoriteRequestDuration,
		metrics.favoriteCountLookupDuration,
		metrics.favoriteCountCacheAccess,
		metrics.chatHistoryRequests,
		metrics.chatHistoryRequestDuration,
		metrics.chatHistoryCacheAccess,
		metrics.rabbitMQQueueMessages,
		metrics.rabbitMQQueueInspectionErrors,
		metrics.rabbitMQConsumerLag,
		metrics.rabbitMQMissingTimestamp,
	)
	return metrics
}

// Handler 返回 Prometheus 文本格式的抓取处理器。
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

// ObserveFavoriteRequest 记录点赞动作和结果；未知值折叠到固定标签，避免标签基数失控。
func (r *Registry) ObserveFavoriteRequest(action, result string, duration time.Duration) {
	action = favoriteActionLabel(action)
	result = requestResultLabel(result)
	r.favoriteRequests.WithLabelValues(action, result).Inc()
	r.favoriteRequestDuration.WithLabelValues(action).Observe(duration.Seconds())
}

// ObserveFavoriteCountLookup 记录一次批量读取点赞计数 Hash 的耗时。
func (r *Registry) ObserveFavoriteCountLookup(duration time.Duration) {
	r.favoriteCountLookupDuration.Observe(duration.Seconds())
}

// ObserveFavoriteCountCacheAccess 记录单个点赞计数 Hash 字段的访问结果。
func (r *Registry) ObserveFavoriteCountCacheAccess(result string) {
	r.favoriteCountCacheAccess.WithLabelValues(cacheResultLabel(result)).Inc()
}

// ObserveChatHistoryRequest 记录聊天历史查询来源、结果和端到端耗时。
func (r *Registry) ObserveChatHistoryRequest(source, result string, duration time.Duration) {
	r.chatHistoryRequests.WithLabelValues(chatSourceLabel(source), requestResultLabel(result)).Inc()
	r.chatHistoryRequestDuration.Observe(duration.Seconds())
}

// ObserveChatHistoryCacheAccess 记录聊天历史缓存访问结果。
func (r *Registry) ObserveChatHistoryCacheAccess(result string) {
	r.chatHistoryCacheAccess.WithLabelValues(cacheResultLabel(result)).Inc()
}

// SetRabbitMQQueueDepth 覆盖固定队列的最近一次积压采样值。
func (r *Registry) SetRabbitMQQueueDepth(queue string, depth int) {
	if !knownQueue(queue) {
		return
	}
	if depth < 0 {
		depth = 0
	}
	r.rabbitMQQueueMessages.WithLabelValues(queue).Set(float64(depth))
}

// ObserveRabbitMQQueueInspectionError 记录固定队列的采样失败次数。
func (r *Registry) ObserveRabbitMQQueueInspectionError(queue string) {
	if knownQueue(queue) {
		r.rabbitMQQueueInspectionErrors.WithLabelValues(queue).Inc()
	}
}

// ObserveRabbitMQConsumerLag 记录固定队列的非负消费延迟。
func (r *Registry) ObserveRabbitMQConsumerLag(queue string, lag time.Duration) {
	if knownQueue(queue) && lag >= 0 {
		r.rabbitMQConsumerLag.WithLabelValues(queue).Observe(lag.Seconds())
	}
}

// ObserveRabbitMQMissingTimestamp 记录无法测算延迟的固定队列消息。
func (r *Registry) ObserveRabbitMQMissingTimestamp(queue string) {
	if knownQueue(queue) {
		r.rabbitMQMissingTimestamp.WithLabelValues(queue).Inc()
	}
}

func favoriteActionLabel(action string) string {
	switch action {
	case "favorite", "unfavorite":
		return action
	default:
		return "other"
	}
}

func requestResultLabel(result string) string {
	switch result {
	case "success", "error":
		return result
	default:
		return "other"
	}
}

func chatSourceLabel(source string) string {
	switch source {
	case "cache", "database":
		return source
	default:
		return "other"
	}
}

func cacheResultLabel(result string) string {
	switch result {
	case "hit", "miss", "error":
		return result
	default:
		return "other"
	}
}

func knownQueue(queue string) bool {
	switch queue {
	case "favorite_user_rpc", "favorite_video_rpc", "cache_invalidation_gateway", "comment_writer", "comment_retry_1s", "comment_dead_letter":
		return true
	default:
		return false
	}
}
