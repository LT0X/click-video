package cache

import "testing"

func TestFavoriteActionResultKeyUsesEventID(t *testing.T) {
	if got, want := favoriteActionResultKey(42), "favorite_action_result:42"; got != want {
		t.Fatalf("favoriteActionResultKey() = %q, want %q", got, want)
	}
}

func TestParseFavoriteActionCount(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
		valid bool
	}{
		{name: "zero", input: "0", want: 0, valid: true},
		{name: "positive", input: "123", want: 123, valid: true},
		{name: "negative", input: "-1"},
		{name: "not a number", input: "count"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, valid := parseFavoriteActionCount(test.input)
			if got != test.want || valid != test.valid {
				t.Fatalf("parseFavoriteActionCount(%q) = (%d, %t), want (%d, %t)", test.input, got, valid, test.want, test.valid)
			}
		})
	}
}

func TestWaitFavoriteActionCountWithoutRedisFallsBack(t *testing.T) {
	originalClient := UserRedisClient
	UserRedisClient = nil
	t.Cleanup(func() { UserRedisClient = originalClient })

	if count, ok := WaitFavoriteActionCount(42); count != 0 || ok {
		t.Fatalf("WaitFavoriteActionCount() = (%d, %t), want (0, false)", count, ok)
	}
}
