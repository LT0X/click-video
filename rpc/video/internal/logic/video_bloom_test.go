package logic

import (
	"reflect"
	"testing"
)

func TestResolveVideoIDsFallsBackForReplicaLocalMisses(t *testing.T) {
	localBloom := map[uint64]bool{10: true}
	database := map[uint64]bool{10: true, 20: true}
	queried := 0
	got, err := resolveVideoIDs([]uint64{10, 20, 30, 0, 20}, func(id uint64) bool {
		return localBloom[id]
	}, func(ids []uint64) ([]uint64, error) {
		queried++
		found := make([]uint64, 0, len(ids))
		for _, id := range ids {
			if database[id] {
				found = append(found, id)
			}
		}
		return found, nil
	}, func(id uint64) { localBloom[id] = true })
	if err != nil {
		t.Fatalf("resolve video IDs: %v", err)
	}
	if !reflect.DeepEqual(got, []uint64{10, 20, 20}) {
		t.Fatalf("resolved IDs = %v, want [10 20 20]", got)
	}
	if queried != 1 || !localBloom[20] {
		t.Fatalf("fallback queries = %d and local filter contains 20 = %t", queried, localBloom[20])
	}
}
