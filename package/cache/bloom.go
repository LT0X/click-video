package cache

import (
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
)

// IDBloomFilter 统一封装 ID 布隆过滤器，并保护启动加载与请求并发访问。
type IDBloomFilter struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
}

func NewIDBloomFilter(expectedElements uint, falsePositiveRate float64) *IDBloomFilter {
	return &IDBloomFilter{filter: bloom.NewWithEstimates(expectedElements, falsePositiveRate)}
}

func (f *IDBloomFilter) AddString(value string) {
	if f == nil || f.filter == nil || value == "" {
		return
	}
	f.mu.Lock()
	f.filter.AddString(value)
	f.mu.Unlock()
}

func (f *IDBloomFilter) TestString(value string) bool {
	if f == nil || f.filter == nil || value == "" {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.filter.TestString(value)
}
