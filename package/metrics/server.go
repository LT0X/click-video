package metrics

import (
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// NewHTTPServer 构建仅暴露 /metrics 的独立 HTTP 服务。
func NewHTTPServer(address string, handler http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	return &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// StartHTTPServer 在独立 goroutine 中启动指标端点，监听失败只记日志，不影响业务服务。
func StartHTTPServer(address, serviceName string, handler http.Handler) *http.Server {
	server := NewHTTPServer(address, handler)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			zap.L().Error("Prometheus 指标 HTTP 服务监听失败",
				zap.String("service", serviceName),
				zap.String("address", address),
				zap.Error(err),
			)
		}
	}()
	return server
}
