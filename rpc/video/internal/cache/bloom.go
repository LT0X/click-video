package cache

import (
	"strconv"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

// IDBloomFilter 由 video.rpc 管理视频 ID 布隆过滤器，避免网关跨域读取 video 表。
type IDBloomFilter struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
}

func NewIDBloomFilter(expectedElements uint, falsePositiveRate float64) *IDBloomFilter {
	return &IDBloomFilter{filter: bloom.NewWithEstimates(expectedElements, falsePositiveRate)}
}

func (filter *IDBloomFilter) AddString(value string) {
	if filter == nil || filter.filter == nil || value == "" {
		return
	}
	filter.mu.Lock()
	filter.filter.AddString(value)
	filter.mu.Unlock()
}

func (filter *IDBloomFilter) TestString(value string) bool {
	if filter == nil || filter.filter == nil || value == "" {
		return false
	}
	filter.mu.RLock()
	defer filter.mu.RUnlock()
	return filter.filter.TestString(value)
}

func VideoExists(videoID uint64) bool {
	return videoID != 0 && VideoIDBloomFilter.TestString(strconv.FormatUint(videoID, 10))
}

func AddVideoID(videoID uint64) {
	if videoID != 0 {
		VideoIDBloomFilter.AddString(strconv.FormatUint(videoID, 10))
	}
}
