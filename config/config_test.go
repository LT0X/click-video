package config

import (
	"path/filepath"
	"testing"
)

func TestApplyUploadDefaultsUsesBoundedDefaults(t *testing.T) {
	previous := System
	t.Cleanup(func() { System = previous })
	System = SystemConfig{HttpAddress: HTTP{VideoAddress: "./media"}}

	applyUploadDefaults()

	if got, want := System.Upload.TempDir, filepath.Join(".", "media", "upload", "tmp"); got != want {
		t.Fatalf("TempDir = %q, want %q", got, want)
	}
	if System.Upload.PartSize != 5*1024*1024 || System.Upload.MaxChunkSize != 10*1024*1024 {
		t.Fatalf("part and chunk sizes = %d/%d, want 5MiB/10MiB", System.Upload.PartSize, System.Upload.MaxChunkSize)
	}
	if System.Upload.MaxParts != 10000 || System.Upload.MaxUploadSize != 5*1024*1024*10000 {
		t.Fatalf("upload limit = %d parts / %d bytes, want 10000 / 5MiB*10000", System.Upload.MaxParts, System.Upload.MaxUploadSize)
	}
	if System.Upload.MergeConcurrency != 3 || System.Upload.TTLHours != 24 || System.Upload.TTLJitterSeconds != 300 || System.Upload.CleanupIntervalMinutes != 30 {
		t.Fatalf("upload lifecycle defaults = %+v", System.Upload)
	}
}

func TestApplyChatDefaultsUsesIsolatedRedisAndRPCAddresses(t *testing.T) {
	previous := System
	t.Cleanup(func() { System = previous })
	System = SystemConfig{UserRedis: Redis{Host: "redis.internal", Port: "6379", PoolSize: 100}}

	applyChatDefaults()

	if System.ChatRedis.Host != "redis.internal" || System.ChatRedis.Port != "6379" || System.ChatRedis.Database != 3 || System.ChatRedis.PoolSize != 100 {
		t.Fatalf("chat Redis defaults = %+v", System.ChatRedis)
	}
	if System.Chat.RPCListenAddress != "0.0.0.0:8014" || System.Chat.AdvertiseAddress != "127.0.0.1:8014" {
		t.Fatalf("chat RPC defaults = %+v", System.Chat)
	}
}
