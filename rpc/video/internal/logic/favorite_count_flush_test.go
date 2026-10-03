package logic

import (
	"reflect"
	"testing"
)

func TestBuildFavoriteCountUpdateSQL(t *testing.T) {
	updates := []favoriteCountUpdate{
		{VideoID: 9, Count: 14},
		{VideoID: 2, Count: 3},
	}

	query, args := buildFavoriteCountUpdateSQL(updates)
	wantQuery := "UPDATE video SET favorite_count = CASE id WHEN ? THEN ? WHEN ? THEN ? ELSE favorite_count END WHERE id IN (?, ?)"
	wantArgs := []interface{}{uint64(2), int64(3), uint64(9), int64(14), uint64(2), uint64(9)}
	if query != wantQuery {
		t.Fatalf("query = %q, want %q", query, wantQuery)
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args = %#v, want %#v", args, wantArgs)
	}
}

func TestBuildFavoriteCountUpdateSQLEmpty(t *testing.T) {
	query, args := buildFavoriteCountUpdateSQL(nil)
	if query != "" || len(args) != 0 {
		t.Fatalf("empty update query = %q, args = %#v", query, args)
	}
}
