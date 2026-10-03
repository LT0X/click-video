package mq

import "testing"

func TestFavoriteActionEventValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   FavoriteActionEvent
		wantErr bool
	}{
		{
			name: "valid favorite",
			event: FavoriteActionEvent{
				EventID: 1, ActionSequence: 1, UserID: 2, AuthorID: 3, VideoID: 4, Cnt: 1,
			},
		},
		{
			name: "valid unfavorite",
			event: FavoriteActionEvent{
				EventID: 1, ActionSequence: 2, UserID: 2, AuthorID: 3, VideoID: 4, Cnt: -1,
			},
		},
		{
			name: "reject unsupported delta",
			event: FavoriteActionEvent{
				EventID: 1, ActionSequence: 1, UserID: 2, AuthorID: 3, VideoID: 4, Cnt: 2,
			},
			wantErr: true,
		},
		{
			name: "reject missing author",
			event: FavoriteActionEvent{
				EventID: 1, ActionSequence: 1, UserID: 2, VideoID: 4, Cnt: 1,
			},
			wantErr: true,
		},
		{
			name: "accept legacy message without action sequence",
			event: FavoriteActionEvent{
				EventID: 1, UserID: 2, AuthorID: 3, VideoID: 4, Cnt: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFavoriteActionEventValidateForPublishRequiresSequence(t *testing.T) {
	event := FavoriteActionEvent{EventID: 1, UserID: 2, AuthorID: 3, VideoID: 4, Cnt: 1}
	if err := event.ValidateForPublish(); err == nil {
		t.Fatal("ValidateForPublish() accepted a new message without an action sequence")
	}
}

func TestFavoriteCountEventValidate(t *testing.T) {
	tests := []struct {
		name    string
		event   FavoriteCountEvent
		wantErr bool
	}{
		{
			name: "valid delta",
			event: FavoriteCountEvent{
				EventID: 1, VideoID: 2, Type: FavoriteCountDelta, Version: 2, Delta: -1, Count: 3,
			},
		},
		{
			name: "valid snapshot",
			event: FavoriteCountEvent{
				EventID: 1, VideoID: 2, Type: FavoriteCountSnapshot, Count: 0,
			},
		},
		{
			name: "reject negative snapshot",
			event: FavoriteCountEvent{
				EventID: 1, VideoID: 2, Type: FavoriteCountSnapshot, Count: -1,
			},
			wantErr: true,
		},
		{
			name: "reject delta without version",
			event: FavoriteCountEvent{
				EventID: 1, VideoID: 2, Type: FavoriteCountDelta, Delta: 1, Count: 1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
