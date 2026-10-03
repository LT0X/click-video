package cache

import "testing"

func TestIDBloomFilterAddsAndFindsIDs(t *testing.T) {
	filter := NewIDBloomFilter(100, 0.01)
	if filter.TestString("video-42") {
		t.Fatal("new bloom filter reported an ID that was never added")
	}
	filter.AddString("video-42")
	if !filter.TestString("video-42") {
		t.Fatal("bloom filter did not retain an added ID")
	}
}

func TestNilIDBloomFilterFailsClosed(t *testing.T) {
	var filter *IDBloomFilter
	filter.AddString("ignored")
	if filter.TestString("ignored") {
		t.Fatal("nil bloom filter should not report IDs as present")
	}
}
