package chat

import (
	"fmt"
	"sync"
)

// Message 是聊天消息在网关、Redis 缓冲和节点间 RPC 中使用的稳定格式。
type Message struct {
	EventID    string `json:"event_id"`
	ID         int64  `json:"id,omitempty"`
	Content    string `json:"content"`
	CreateTime int64  `json:"create_time"`
	FromUserID uint64 `json:"from_user_id"`
	ToUserID   uint64 `json:"to_user_id"`
}

// SocketConn 收窄 WebSocket 依赖，便于对并发投递做单元测试。
type SocketConn interface {
	WriteJSON(interface{}) error
	Close() error
}

// Client 对同一 WebSocket 的写操作加锁，避免心跳响应和消息推送并发写帧。
type Client struct {
	conn    SocketConn
	writeMu sync.Mutex
}

func NewClient(conn SocketConn) *Client { return &Client{conn: conn} }

func (c *Client) WriteJSON(value interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(value)
}

func (c *Client) Close() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.Close()
}

// Registry 维护本节点在线用户连接。一个用户只保留最新的活动连接。
type Registry struct {
	mu      sync.RWMutex
	clients map[uint64]*Client
}

func NewRegistry() *Registry { return &Registry{clients: make(map[uint64]*Client)} }

func (r *Registry) Register(userID uint64, client *Client) {
	r.mu.Lock()
	previous := r.clients[userID]
	r.clients[userID] = client
	r.mu.Unlock()
	if previous != nil && previous != client {
		_ = previous.Close()
	}
}

// Remove 只删除仍指向该连接的用户记录，旧连接迟到的断开事件不能清除新连接。
func (r *Registry) Remove(userID uint64, client *Client) {
	r.mu.Lock()
	if r.clients[userID] == client {
		delete(r.clients, userID)
	}
	r.mu.Unlock()
}

func (r *Registry) Client(userID uint64) (*Client, bool) {
	r.mu.RLock()
	client, ok := r.clients[userID]
	r.mu.RUnlock()
	return client, ok
}

func (r *Registry) Deliver(userID uint64, msg Message) error {
	client, ok := r.Client(userID)
	if !ok {
		return fmt.Errorf("用户 %d 不在本节点在线连接表中", userID)
	}
	return client.WriteJSON(struct {
		Type string `json:"type"`
		Message
	}{Type: "message", Message: msg})
}
