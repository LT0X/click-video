package chat

import (
	"testing"
	"time"
)

func TestRouteLeaseKeepsAddressAndConnectionIdentity(t *testing.T) {
	const lease = "[2001:db8::1]:8014|c3a74f"
	address, token, ok := parseRouteLease(lease)
	if !ok || address != "[2001:db8::1]:8014" || token != "c3a74f" {
		t.Fatalf("parsed lease = %q, %q, %v", address, token, ok)
	}
	if RouteKey(82) != "chat:route:82" {
		t.Fatalf("route key = %q", RouteKey(82))
	}
	if RouteTTL != 30*time.Second {
		t.Fatalf("route TTL = %s, want 30s", RouteTTL)
	}
}

func TestRouteLeaseRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "node", "|token", "node|"} {
		if _, _, ok := parseRouteLease(value); ok {
			t.Fatalf("parseRouteLease(%q) unexpectedly succeeded", value)
		}
	}
}
