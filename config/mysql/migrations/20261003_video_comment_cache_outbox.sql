-- 评论增删与缓存失效 Outbox 同事务写入，避免数据库提交后进程崩溃造成缓存永久陈旧。
CREATE TABLE IF NOT EXISTS `comment_cache_invalidation_outbox` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `video_id` bigint unsigned NOT NULL,
  `created_at` datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
