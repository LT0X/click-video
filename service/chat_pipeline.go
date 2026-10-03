package service

import (
	"sync"

	"douyin/package/chat"
)

var chatPipelineState struct {
	sync.RWMutex
	dispatcher *chat.Dispatcher
	cache      *chat.Cache
}

func ConfigureChatPipeline(dispatcher *chat.Dispatcher, cache *chat.Cache) {
	chatPipelineState.Lock()
	chatPipelineState.dispatcher = dispatcher
	chatPipelineState.cache = cache
	chatPipelineState.Unlock()
}

func currentChatPipeline() (*chat.Dispatcher, *chat.Cache) {
	chatPipelineState.RLock()
	dispatcher, cache := chatPipelineState.dispatcher, chatPipelineState.cache
	chatPipelineState.RUnlock()
	return dispatcher, cache
}
