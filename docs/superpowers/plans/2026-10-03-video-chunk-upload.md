# 大视频分片上传 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现本地磁盘版分片上传、断点续传、秒传、顺序合并和前端三路并发上传，同时保持既有发布 API 可用。

**Architecture:** Fiber 网关用 request body stream 接收原始二进制分片并通过 `io.Copy` 写入本地临时目录；Redis 保存上传元数据和已传分片。合并器用有界缓冲区顺序合并并计算整体 MD5，完成后通过 video.rpc 创建视频记录、通过 user.rpc 幂等更新作品数。前端 Web Worker 增量计算 MD5，File.slice 分片后最多三路并发上传。

**Tech Stack:** Go 1.20、Fiber v2.49.2、go-redis、MySQL/GORM、gRPC/go-zero、React 18、Web Worker、SparkMD5。

**Spec:** `temp/optimize_doc/大视频上传优化.md`（仓库忽略的设计稿）以及用户 goal 中的大视频上传任务和验收项。

## Global Constraints

- 不改变既有 API 路径和响应结构；新增 `/api/upload/init`、`/api/upload/chunk`、`/api/upload/merge`。
- 单分片以流方式写本地磁盘；合并以固定大小缓冲区顺序处理；后端单请求内存目标低于 2MB。
- Redis 上传状态有效期以 24h 为基础并增加随机偏移；临时目录每 30 分钟清理过期项；不使用 Redis `KEYS`。
- 最大合并并发为 3；客户端分片大小为 5MiB、客户端 MD5 分块读取为 2MiB、分片数上限为 10,000（可上传至约 50GiB）。
- user 表只由 user.rpc 更新，video 表只由 video.rpc 更新；新增 Redis key 和配置同步登记到 README。
- 中文注释；新增依赖仅限于浏览器 MD5 所需的 SparkMD5，不引入 Go 运行时依赖。
- 不提交 `temp/`、`.gitignore` 改动或凭据；逐文件暂存。

## Review Focus

- 非 UUID 或路径穿越形式的 upload ID、越界分片号和超限请求必须在文件系统访问前拒绝；由 `package/upload` 验证测试覆盖。
- 重复并发分片不得留下半截目标文件；相同分片重试应幂等；由存储管理器测试覆盖。
- 缺失分片、总大小错误或整体 MD5 不匹配时不得发布视频记录；由合并测试覆盖。
- 视频记录创建成功但 user.rpc 或 Redis 后续步骤失败时，重试不得重复建视频或重复增加作品数；由 RPC 幂等状态测试覆盖。
- Redis 已记录分片但本地文件缺失时，续传列表不得把该分片报告为已完成；由状态服务测试覆盖。

---

### Task 1: 本地分片存储与顺序合并

**Files:**
- Create: `package/upload/manager.go`
- Test: `package/upload/manager_test.go`

**Interfaces:**
- Produces `NewManager(Config) *Manager`、`(*Manager).WritePart(uploadID string, partNumber, totalParts int, src io.Reader, declaredSize int64) (string, error)`、`(*Manager).Merge(uploadID string, totalParts int, expectedSize int64, expectedMD5 string) (string, error)`。
- 分片路径固定为 `part_%05d`；中间文件写完并校验长度后原子改名。合并以 256KiB bufio 缓冲区按 1..N 顺序写入临时输出，并在同一遍流式计算 MD5。

- [x] 写测试：`TestWritePartStreamsAndUsesPartNumberName` 检查逐块 reader、落盘内容和 `part_00001` 命名。
- [x] 写测试：`TestWritePartAllocatesLessThanTwoMiBForLargeReader` 用不缓存内容的 10MiB reader 验证 Go 分片写入分配低于 2MiB。
- [x] 写测试：`TestMergeOrdersPartsAndVerifiesMD5` 以乱序创建的分片验证顺序、最终字节、大小与 MD5。
- [x] 写测试：`TestMergeRejectsMissingPartAndWrongMD5` 验证缺片和错误 MD5 均报错且不留下正式文件。
- [x] 写测试：`TestMergeLimitAllowsAtMostThree` 验证同时运行的合并最多为 3。
- [x] 逐个运行上述测试确认先因缺少实现而失败，再实现最小文件管理器使测试通过。
- [x] 提交 Task 1：`feat(upload): 分片写入与校验合并`。

### Task 2: Redis 上传状态、参数校验与过期清理

**Files:**
- Create: `service/upload_state.go`
- Create: `service/upload_state_test.go`
- Modify: `config/config.go`
- Modify: `config/config.yaml`
- Modify: `main.go`

**Interfaces:**
- 状态服务创建/读取上传元数据、读取已完成分片、标记分片、设置合并/完成状态、查秒传映射。
- Key: `upload:{uploadID}`（Hash）、`upload_parts:{uploadID}`（Set）、`upload_md5:{md5}`（String）。元数据和分片集合使用同一 24h 基础 TTL 与随机偏移。
- 清理任务扫描配置的本地临时目录，移除超过 TTL 的上传目录；已知目录名可推导 Redis key，不执行 `KEYS`。

- [ ] 写测试：参数校验拒绝无效 MD5、非法 part count、负大小和超过 10,000 片上限的文件。
- [ ] 写测试：状态服务只在文件存在时返回“已上传分片”，并在新分片完成后登记状态。
- [ ] 写测试：30 分钟清理逻辑删除过期目录并保留未过期/近期活跃目录。
- [ ] 写测试确认失败，再实现 Redis 状态适配器、配置默认值和清理 worker。
- [ ] 提交 Task 2：`feat(upload): 上传状态续传与过期清理`。

### Task 3: 视频发布的 RPC 所有权与幂等性

**Files:**
- Modify: `rpc/video/video.proto`
- Modify: `rpc/video/video/video.pb.go`
- Modify: `rpc/video/video/video_grpc.pb.go`
- Modify: `rpc/video/internal/model/video.go`
- Modify: `rpc/video/internal/logic/create_video_logic.go`
- Test: `rpc/video/internal/logic/create_video_logic_test.go`
- Modify: `rpc/user/user.proto`
- Modify: generated user protobuf/client/server files
- Create: `rpc/user/internal/model/work_count_video.go`
- Create: `rpc/user/internal/logic/increment_work_count_logic.go`
- Test: `rpc/user/internal/logic/increment_work_count_logic_test.go`
- Modify: `rpc/user/internal/server/user_server.go`
- Modify: `service/publish.go`
- Modify: `config/mysql/douyin.sql`
- Create: `config/mysql/migrations/20261003_video_upload.sql`

**Interfaces:**
- `Video.CreateVideoRequest` 增加 `UploadID`；video.rpc 以唯一 upload ID 幂等插入视频并返回 ID。
- 新增 user.rpc `IncrementWorkCount(UserID, VideoID)`；通过 user 域的唯一 video ledger 保证重试只增加一次 `work_count`。
- 既有发布服务和新合并流程都经 RPC 写入各服务表，不在网关直接创建 video 或更新 user 表。

- [ ] 写测试：upload ID 重试解析到同一视频 ID；空标题/作者/必需 URL 被拒绝。
- [ ] 写测试：同一 video ID 重试作品数更新只应用一次，未知用户返回错误。
- [ ] 确认测试失败后，实现 RPC schema、数据库唯一约束、服务逻辑和 generated stubs。
- [ ] 使既有 `/douyin/publish/action/` 改走 video.rpc/user.rpc，保留请求路径和 `CommonResponse` 字段。
- [ ] 提交 Task 3：`feat(upload): 通过 RPC 幂等发布视频`。

### Task 4: HTTP init/chunk/merge 与 Fiber 流式接入

**Files:**
- Create: `handler/upload.go`
- Create: `handler/upload_test.go`
- Create: `service/upload.go`
- Modify: `router/route.go`
- Modify: `main.go`
- Modify: `package/util/ffmpges.go`

**Interfaces:**
- init 接 JSON：`file_name`、`file_size`、`file_md5`、`total_parts`、`title`、`topic`；从 token header 鉴权。
- chunk 用原始 `application/octet-stream` body，upload ID 和分片号走 query；handler 把 body stream 传给 `WritePart`。
- merge 验证归属、完整分片和 MD5；取得三并发信号量后合并、创建 video/user RPC 记录并保存秒传映射。成功后清理分片目录，封面提取异步执行并经 video.rpc 回写 URL。
- Fiber 开启 `StreamRequestBody`，保持现有 30MiB 全局上限；分片上限由上传配置单独校验。

- [ ] 写 HTTP 测试：init 返回可续传 upload ID 和已上传列表；秒传命中返回已存在视频。
- [ ] 写 HTTP 测试：raw body chunk 确认为 Fiber request stream 并写盘；无效 token、越界分片号和非 uploading 状态被拒绝。
- [ ] 写 HTTP 测试：合并完整成功；缺片/错 MD5 不调用视频发布 RPC。
- [ ] 确认测试失败后实现 handler/service/router、路径化封面输出和 Fiber stream 配置。
- [ ] 提交 Task 4：`feat(upload): 增加流式分片上传接口`。

### Task 5: React Worker 与可续传上传器

**Files:**
- Modify: `frontend/package.json`
- Modify: `frontend/package-lock.json`
- Create: `frontend/src/workers/uploadHash.worker.js`
- Create: `frontend/src/utils/chunkUpload.js`
- Modify: `frontend/src/component/UploadPopover.js`
- Create: `frontend/src/utils/chunkUpload.test.js`

**Interfaces:**
- Worker 用 2MiB `File.slice` 递增计算完整 MD5 并回传进度。
- `uploadFileInChunks(file, metadata, token, onProgress)` 发送 init、仅上传未完成分片、最多 3 并发、单片失败指数退避重试，最后 merge。
- 5MiB 分片；请求将 Blob 作为 raw body 发送到 `/api/upload/chunk`，token 通过 header 传递。

- [ ] 写测试：跳过 init 返回的已上传分片；失败分片按指数间隔重试并最终完成。
- [ ] 写测试确认并发上传峰值不超过 3，MD5 worker 分块而非整文件读取。
- [ ] 确认测试失败后加入 SparkMD5 并完成 worker、上传队列和上传弹窗接线。
- [ ] 提交 Task 5：`feat(upload): 前端分片续传与并发重试`。

### Task 6: 文档与完整验收

**Files:**
- Modify: `README.md`
- Review: `temp/optimize_doc/简历.txt`（忽略文件，只核对，不提交）

- [ ] README 登记上传路由、配置、Redis keys、24h+jitter、chunk raw-body 限制和本地存储目录。
- [ ] 运行 `go build ./...`、`go vet ./...`、`go test ./...`、前端测试和 production build。
- [ ] 检查单分片服务使用请求流和有界缓冲，没有 `FormFile`/整片 `ReadAll`/整文件 `[]byte` 路径；检查合并最大并发为 3。
- [ ] 运行 `git diff --check`，确认暂存清单没有 `.gitignore`、`temp/`、`.txt` 或凭据，再提交、推送并创建该任务独立 PR。
- [ ] 提交 Task 6：`feat(upload): 补充上传配置与使用文档`。
