package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

const (
	HistoryLimit = 50
	historyTTL   = time.Hour
	newestTTL    = 10 * time.Minute
)

type Cache struct{ client *redis.Client }

func NewCache(client *redis.Client) *Cache { return &Cache{client: client} }

func HistoryKey(userID, toUserID uint64) string {
	if userID > toUserID {
		userID, toUserID = toUserID, userID
	}
	return fmt.Sprintf("chat:history:%d:%d", userID, toUserID)
}

func HistoryCompleteKey(userID, toUserID uint64) string {
	return HistoryKey(userID, toUserID) + ":complete"
}

func NewestKey(userID, toUserID uint64) string {
	return fmt.Sprintf("chat:message_newest:%d:%d", userID, toUserID)
}

func CanMarkHistoryComplete(count int) bool { return count > 0 && count < HistoryLimit }

func ChatCacheTTL(base time.Duration) time.Duration {
	return base + time.Duration(rand.Int63n(int64(5*time.Minute)+1))
}

func (c *Cache) GetHistory(_ context.Context, userID, toUserID uint64, preMessageTime int64) ([]Message, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, fmt.Errorf("聊天 Redis 缓存未初始化")
	}
	complete, err := c.client.Get(HistoryCompleteKey(userID, toUserID)).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("读取聊天历史缓存标志失败: %w", err)
	}
	if complete != "1" {
		return nil, false, nil
	}
	minScore := "-inf"
	if preMessageTime > 0 {
		minScore = "(" + strconv.FormatInt(preMessageTime, 10)
	}
	members, err := c.client.ZRangeByScore(HistoryKey(userID, toUserID), redis.ZRangeBy{
		Min: minScore, Max: "+inf", Count: HistoryLimit,
	}).Result()
	if err != nil {
		return nil, false, fmt.Errorf("读取聊天历史 ZSet 失败: %w", err)
	}
	messages := make([]Message, 0, len(members))
	for _, member := range members {
		var msg Message
		if err := json.Unmarshal([]byte(member), &msg); err != nil {
			_ = c.client.Del(HistoryCompleteKey(userID, toUserID)).Err()
			return nil, false, nil
		}
		messages = append(messages, msg)
	}
	return messages, true, nil
}

// FillHistory 只在 DB 返回少于 50 条时标记缓存完整；达到上限的结果可能被截断，必须继续查 contact.rpc。
func (c *Cache) FillHistory(_ context.Context, userID, toUserID uint64, messages []Message) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("聊天 Redis 缓存未初始化")
	}
	key := HistoryKey(userID, toUserID)
	ttl := ChatCacheTTL(historyTTL)
	pipe := c.client.TxPipeline()
	for _, msg := range messages {
		member, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("序列化聊天历史缓存失败: %w", err)
		}
		pipe.ZAdd(key, redis.Z{Score: float64(msg.CreateTime), Member: string(member)})
	}
	pipe.Expire(key, ttl)
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("回填聊天历史 ZSet 失败: %w", err)
	}
	if err := trimHistory(c.client, key); err != nil {
		return err
	}
	count, err := c.client.ZCard(key).Result()
	if err != nil {
		return fmt.Errorf("统计聊天历史 ZSet 失败: %w", err)
	}
	marker := HistoryCompleteKey(userID, toUserID)
	if CanMarkHistoryComplete(len(messages)) && count == int64(len(messages)) {
		if err := c.client.Set(marker, "1", ttl).Err(); err != nil {
			return fmt.Errorf("设置聊天历史完整缓存标志失败: %w", err)
		}
		return nil
	}
	if err := c.client.Del(marker).Err(); err != nil {
		return fmt.Errorf("删除不完整聊天历史标志失败: %w", err)
	}
	return nil
}

func (c *Cache) OnMessageQueued(_ context.Context, msg Message) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("聊天 Redis 缓存未初始化")
	}
	if err := validateMessage(msg); err != nil {
		return err
	}
	// 历史 ZSet 只从 DB 查询结果回填，避免队列消息和稍后分配 DB ID 的同一消息重复入缓存。
	return c.updateNewest([]Message{msg})
}

func trimHistory(client *redis.Client, key string) error {
	_, err := redis.NewScript(`
local count = redis.call('ZCARD', KEYS[1])
if count > tonumber(ARGV[1]) then
  return redis.call('ZREMRANGEBYRANK', KEYS[1], 0, count - tonumber(ARGV[1]) - 1)
end
return 0
`).Run(client, []string{key}, HistoryLimit).Result()
	if err != nil {
		return fmt.Errorf("限制聊天历史缓存条数失败: %w", err)
	}
	return nil
}

func (c *Cache) GetNewest(_ context.Context, userID, toUserID uint64) (Message, bool, error) {
	if c == nil || c.client == nil {
		return Message{}, false, fmt.Errorf("聊天 Redis 缓存未初始化")
	}
	members, err := c.client.ZRevRange(NewestKey(userID, toUserID), 0, 0).Result()
	if err == redis.Nil {
		return Message{}, false, nil
	}
	if err != nil {
		return Message{}, false, fmt.Errorf("读取好友最新消息缓存失败: %w", err)
	}
	if len(members) == 0 {
		return Message{}, false, nil
	}
	var msg Message
	if err := json.Unmarshal([]byte(members[0]), &msg); err != nil {
		_ = c.client.Del(NewestKey(userID, toUserID)).Err()
		return Message{}, false, nil
	}
	return msg, true, nil
}

// UpdateNewest 以 ZSet 保留最近一条，Redis 事务保证并发消息不会因普通 SET 的乱序回写而覆盖较新内容。
func (c *Cache) UpdateNewest(msg Message) error {
	return c.updateNewest([]Message{msg})
}

// AfterPersist 在 DB 提交后失效历史完整标志并刷新好友最新消息；失败会让 Stream 保留待重试。
func (c *Cache) AfterPersist(messages []Message) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("聊天 Redis 缓存未初始化")
	}
	return c.updateNewest(messages)
}

func (c *Cache) updateNewest(messages []Message) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("聊天 Redis 缓存未初始化")
	}
	if len(messages) == 0 {
		return nil
	}
	keys := make(map[string]struct{}, len(messages)*3)
	pipe := c.client.TxPipeline()
	for _, msg := range messages {
		payload, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("序列化好友最新消息失败: %w", err)
		}
		pipe.Del(HistoryCompleteKey(msg.FromUserID, msg.ToUserID))
		for _, key := range []string{NewestKey(msg.FromUserID, msg.ToUserID), NewestKey(msg.ToUserID, msg.FromUserID)} {
			pipe.ZAdd(key, redis.Z{Score: float64(msg.CreateTime), Member: string(payload)})
			pipe.Expire(key, ChatCacheTTL(newestTTL))
			keys[key] = struct{}{}
		}
	}
	if _, err := pipe.Exec(); err != nil {
		return fmt.Errorf("更新好友最新消息和失效聊天历史缓存失败: %w", err)
	}
	keyList := make([]string, 0, len(keys))
	for key := range keys {
		keyList = append(keyList, key)
	}
	if len(keyList) > 0 {
		if _, err := redis.NewScript(`
for i = 1, #KEYS do
  local count = redis.call('ZCARD', KEYS[i])
  if count > 1 then
    redis.call('ZREMRANGEBYRANK', KEYS[i], 0, count - 2)
  end
end
return 1
`).Run(c.client, keyList).Result(); err != nil {
			return fmt.Errorf("限制好友最新消息缓存条数失败: %w", err)
		}
	}
	return nil
}
