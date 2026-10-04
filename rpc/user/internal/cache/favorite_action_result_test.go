package cache

import "testing"

func TestFavoriteActionResultKeyUsesEventID(t *testing.T) {
	if got, want := favoriteActionResultKey(42), "favorite_action_result:42"; got != want {
		t.Fatalf("favoriteActionResultKey() = %q, want %q", got, want)
	}
}

func TestSetFavoriteActionResultRejectsInvalidValues(t *testing.T) {
	if err := SetFavoriteActionResult(0, 1); err == nil {
		t.Fatal("expected zero event ID to be rejected")
	}
	if err := SetFavoriteActionResult(42, -1); err == nil {
		t.Fatal("expected negative count to be rejected")
	}
}
