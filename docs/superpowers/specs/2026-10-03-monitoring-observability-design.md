# 监控与收尾设计

## 状态

设计已于 2026-10-03 在对话中确认，待用户审阅本文档。

## 目标与范围

为 P3 收尾提供可由 Prometheus 抓取的关键运行指标，并补齐 README 中配置和 Redis Key 清单，核对 `temp/optimize_doc/简历.txt` 中每个亮点与当前代码的对应关系。指标覆盖优化文档第 8.4 节：点赞 RT/QPS/错误率、Count 查询 RT/缓存命中率、聊天历史查询 RT/缓存命中率，以及 MQ 队列积压和消费延迟。

不改变现有业务 API 路径和响应，不新增 Redis Key，不引入新的外部服务或告警系统，不报告未经测量的性能提升数字。

## 架构与数据流

新增内部 `package/metrics`，集中定义 Prometheus 指标、注册表和 HTTP handler。使用现有依赖树中的 `github.com/prometheus/client_golang`；代码直接导入后将其从间接依赖调整为直接依赖，不新增模块或版本。

网关在点赞/取消点赞请求完成时记录请求总数、耗时和错误结果；在聊天历史查询时记录端到端耗时、缓存 hit/miss/error 及最终成功/失败。网关定时通过 RabbitMQ `QueueInspect` 采样固定队列的待处理消息数，记录为 gauge；采样周期为 5 秒。

video.rpc 在 `LoadFavoriteCounts` 批量读取 Redis Hash 时记录 lookup 耗时，并按字段记录 hit、miss 或 error。RabbitMQ 消费者在 handler 执行前根据发布时写入的 AMQP timestamp 记录消息排队/重试延迟。网关、user.rpc 和 video.rpc 各自启动独立 `net/http` metrics server，暴露标准 `/metrics` 路径；Prometheus 分别抓取三个进程。Contact RPC 不消费本方案涉及的 RabbitMQ 队列，也不新增 metrics listener。

指标名：

| 指标 | 类型 | 说明 |
|------|------|------|
| `click_video_favorite_requests_total` | Counter | 点赞及取消点赞请求数，标签为 `action`、`result` |
| `click_video_favorite_request_duration_seconds` | Histogram | 点赞及取消点赞请求耗时，标签为 `action` |
| `click_video_favorite_count_lookup_duration_seconds` | Histogram | video.rpc 批量读取点赞计数 Hash 的耗时 |
| `click_video_favorite_count_cache_access_total` | Counter | 点赞计数 Hash 字段访问结果，标签 `result=hit\|miss\|error` |
| `click_video_chat_history_requests_total` | Counter | 聊天历史查询结果，标签 `source=cache\|database`、`result=success\|error` |
| `click_video_chat_history_request_duration_seconds` | Histogram | 聊天历史端到端查询耗时 |
| `click_video_chat_history_cache_access_total` | Counter | 聊天历史缓存结果，标签 `result=hit\|miss\|error` |
| `click_video_rabbitmq_queue_messages` | Gauge | 最近一次采样的队列积压量，标签为固定队列名 |
| `click_video_rabbitmq_queue_inspection_errors_total` | Counter | 队列长度采样失败次数，标签为固定队列名 |
| `click_video_rabbitmq_consumer_lag_seconds` | Histogram | 从消息初次发布到消费者开始处理的时间，标签为固定队列名 |
| `click_video_rabbitmq_consumer_messages_missing_timestamp_total` | Counter | 没有 AMQP timestamp、因此无法测算延迟的既有消息数，标签为固定队列名 |

点赞 QPS 和各耗时分位数由 Prometheus 基于 Counter 与 Histogram 查询计算；缓存命中率可由 hit 与 miss 计数计算。项目不自行维护滑动窗口或高基数序列。

## 配置与安全

网关在 `config/config.yaml` 增加 `monitoring.listenAddress`，默认 `127.0.0.1:9100`；video.rpc 在 `rpc/video/etc/video.yaml` 增加 `MetricsListenOn`，默认 `127.0.0.1:9113`；user.rpc 在 `rpc/user/etc/user.yaml` 增加 `MetricsListenOn`，默认 `127.0.0.1:9112`。若 Prometheus 在独立主机或容器中抓取，运维人员可将监听地址改为私网地址，并通过防火墙限制访问。

只使用固定动作、结果、队列名作为标签，禁止用户 ID、视频 ID、错误文本进入标签。指标仅驻留进程内，不写 Redis 或数据库。metrics server 启动或监听失败时记录清晰错误日志，不改变业务 RPC/API 的可用性。

## 队列采样范围

网关每 5 秒采样以下既有队列：`favorite_user_rpc`、`favorite_video_rpc`、`cache_invalidation_gateway`、`comment_writer`、`comment_retry_1s`、`comment_dead_letter`。user.rpc、video.rpc 和网关的 RabbitMQ 消费入口均记录 delivery lag。所有新发布消息由公共 publisher 在 timestamp 缺失时补充 UTC 发布时刻；已有无 timestamp 的消息只增加 missing-timestamp 计数，不伪造延迟。采样失败增加对应错误计数并保留最近一次 gauge 值；队列名固定白名单，避免无界标签。

## README 与简历核对

README 记录三个 metrics 地址配置、指标含义、抓取方式、固定队列清单、QPS/RT/缓存命中率 PromQL 示例及 Redis Key 清单。逐项对照 `简历.txt` 的微服务、Feed/MapReduce、分片上传、热点视频和聊天描述；只保留有代码证据的表述，并指出无法从代码验证的性能数字或表述偏差。`temp/` 下的设计文档和简历文件继续保持 git 忽略状态，不纳入 PR。

## 验证

- 单测覆盖指标递增、耗时记录、缓存结果分类、队列 gauge 更新/采样失败、带/不带 timestamp 的 consumer lag，以及 `/metrics` 格式输出。
- `go build ./...`、`go vet ./...`、`go test ./...` 全部通过。
- PR 前执行独立子 agent 对抗审查，模型限定为 `gpt-6-luna`、推理强度 `max`。

## 取舍

复用 Prometheus 标准客户端，避免手写文本编码、注册和 Histogram 聚合逻辑。指标端点与业务监听分离，默认 loopback 减少意外暴露；无新 Redis Key，因此 P3 只需核对并补全已有 Key 文档。
