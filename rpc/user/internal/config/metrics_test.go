package config

import "testing"

func TestMetricsListenAddressDefaultsToLoopback(t *testing.T) {
	if got := (Config{}).MetricsListenAddress(); got != "127.0.0.1:9112" {
		t.Fatalf("empty metrics address resolved to %q", got)
	}
	if got := (Config{MetricsListenOn: "0.0.0.0:9112"}).MetricsListenAddress(); got != "0.0.0.0:9112" {
		t.Fatalf("configured metrics address changed to %q", got)
	}
}
