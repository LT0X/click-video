package cache

import "testing"

func TestVideoIDBloomFilterTracksNewAndExistingIDs(t *testing.T) {
	previous := VideoIDBloomFilter
	t.Cleanup(func() { VideoIDBloomFilter = previous })
	VideoIDBloomFilter = NewIDBloomFilter(100, 0.01)

	if VideoExists(0) || VideoExists(42) {
		t.Fatal("empty video filter unexpectedly accepted an ID")
	}
	AddVideoID(42)
	if !VideoExists(42) {
		t.Fatal("video filter did not retain an added ID")
	}
}

func TestNilVideoIDBloomFilterFailsClosed(t *testing.T) {
	previous := VideoIDBloomFilter
	VideoIDBloomFilter = nil
	t.Cleanup(func() { VideoIDBloomFilter = previous })

	if VideoExists(42) {
		t.Fatal("nil video filter unexpectedly accepted an ID")
	}
	AddVideoID(42)
}
