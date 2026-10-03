# 聊天系统优化实施计划

> 按仓库约定，本任务单独使用 `feat/chat-message-optimization` 分支和一个 PR。

**目标：** WebSocket 在线直推与跨节点路由、Redis 持久缓冲后批量写入 contact.rpc、聊天历史与好友最新消息缓存，保持现有 HTTP API 路径和响应结构。

**架构：** 网关实例维护本地连接表并托管内部 gRPC 投递端点；Redis 保存带 TTL 的在线路由和消息 Stream。普通消息、AI 提问和 AI 回复都在写入 Stream 成功后才确认发送，Worker 以消费组攒批调用 contact.rpc，成功后 ACK，失败保留待重试记录。消息表仅由 contact.rpc 批量幂等写入，AI 用户初始化通过 user.rpc。历史 ZSet 与会话最新消息缓存只作缓存，DB/RPC 仍为持久真相源。

## 实施顺序

1. 为本地连接注册/移除、在线投递、Stream 消息编解码、批量持久化确认和历史缓存完整性写单测，先运行确认失败。
2. 增加 Redis Stream 缓冲及消费者组批处理；增加 contact.rpc 批量幂等写接口与迁移，数据库成功后才 ACK。
3. 增加 gateway 节点 gRPC 推送端点、Redis 用户路由 TTL 续期、WebSocket ping/post 协议和连接生命周期清理。
4. 接入聊天历史 ZSet 异步回填、会话最新消息缓存，并让好友列表使用最新消息缓存。
5. 前端接入 WebSocket 实时收发与每 10 秒心跳，保留现有 REST 历史/好友接口和响应结构。
6. 更新配置和 README Redis Key 表，运行 Go build/vet/test 与前端相关测试/构建，逐文件暂存并提交 PR。

## 重点验证

- Redis 写入失败时不向发送方报告成功；DB 批量写失败时 Stream 记录不 ACK；重复投递靠事件 ID 幂等。
- 断开旧连接不能删除新连接的路由；本地、远端、离线三条路由分别覆盖。
- 路由 TTL 为 30 秒，心跳续期；消息历史缓存最多 50 条，只有确认完整时才能命中，避免截断旧历史。
- 所有跨域持久化通过 contact.rpc；不在网关直接读写 message 表；不使用 Redis `KEYS`。
