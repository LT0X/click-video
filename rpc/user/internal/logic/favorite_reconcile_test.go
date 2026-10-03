package logic

import (
	"reflect"
	"testing"
)

func TestBuildFavoriteCountSnapshotsIncludesZeroCounts(t *testing.T) {
	counts := []favoriteVideoCountRow{
		{VideoID: 2, Count: 3},
		{VideoID: 9, Count: 0},
		{VideoID: 11, Count: 5},
	}
	versions := []favoriteVideoCountVersionRow{
		{VideoID: 2, CountVersion: 4},
		{VideoID: 9, CountVersion: 8},
		{VideoID: 12, CountVersion: 1},
	}

	got := buildFavoriteCountSnapshots(counts, versions)
	want := []favoriteVideoCountSnapshot{
		{VideoID: 2, Count: 3, CountVersion: 4},
		{VideoID: 9, Count: 0, CountVersion: 8},
		{VideoID: 11, Count: 5},
		{VideoID: 12, Count: 0, CountVersion: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshots = %#v, want %#v", got, want)
	}
}
