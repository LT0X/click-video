package chat

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

const RouteTTL = 30 * time.Second

type RedisRoutes struct{ client *redis.Client }

func NewRedisRoutes(client *redis.Client) *RedisRoutes { return &RedisRoutes{client: client} }

func RouteKey(userID uint64) string { return fmt.Sprintf("chat:route:%d", userID) }

func (r *RedisRoutes) Register(_ context.Context, userID uint64, nodeAddress string) (string, error) {
	if userID == 0 || nodeAddress == "" {
		return "", fmt.Errorf("聊天路由参数无效")
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", fmt.Errorf("生成聊天路由租约失败: %w", err)
	}
	lease := nodeAddress + "|" + hex.EncodeToString(random[:])
	if err := r.client.Set(RouteKey(userID), lease, RouteTTL).Err(); err != nil {
		return "", fmt.Errorf("注册聊天在线路由失败: %w", err)
	}
	return lease, nil
}

func (r *RedisRoutes) Lookup(_ context.Context, userID uint64) (string, error) {
	address, err := r.client.Get(RouteKey(userID)).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("查询聊天在线路由失败: %w", err)
	}
	address, _, ok := parseRouteLease(address)
	if !ok {
		return "", fmt.Errorf("聊天路由值格式错误")
	}
	return address, nil
}

// Refresh 与 Remove 比较每条连接独有的租约，防止旧连接续期或删除新连接的路由。
func (r *RedisRoutes) Refresh(_ context.Context, userID uint64, lease string) (bool, error) {
	result, err := redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
	`).Run(r.client, []string{RouteKey(userID)}, lease, strconv.Itoa(int(RouteTTL/time.Second))).Int()
	if err != nil {
		return false, fmt.Errorf("续期聊天在线路由失败: %w", err)
	}
	return result == 1, nil
}

func (r *RedisRoutes) Remove(_ context.Context, userID uint64, lease string) error {
	_, err := redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
	`).Run(r.client, []string{RouteKey(userID)}, lease).Result()
	if err != nil {
		return fmt.Errorf("删除聊天在线路由失败: %w", err)
	}
	return nil
}

func parseRouteLease(lease string) (string, string, bool) {
	separator := strings.LastIndex(lease, "|")
	if separator <= 0 || separator == len(lease)-1 {
		return "", "", false
	}
	return lease[:separator], lease[separator+1:], true
}
