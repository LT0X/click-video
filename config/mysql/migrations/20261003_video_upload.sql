-- 视频由 video.rpc 独占写入；upload_id 让合并重试复用已创建的视频。
ALTER TABLE `video`
  ADD COLUMN `upload_id` varchar(36) NULL AFTER `id`;

-- 兼容迁移前已有视频，同时保证唯一索引可以建立。
UPDATE `video`
SET `upload_id` = CONCAT('legacy-', `id`)
WHERE `upload_id` IS NULL;

ALTER TABLE `video`
  MODIFY COLUMN `upload_id` varchar(36) NOT NULL,
  ADD UNIQUE KEY `uk_video_upload_id` (`upload_id`);

-- user.rpc 在此表登记已计入作品数的视频，避免 RPC 重试重复加一。
CREATE TABLE `work_count_video` (
  `video_id` bigint unsigned NOT NULL,
  `user_id` bigint unsigned NOT NULL,
  PRIMARY KEY (`video_id`),
  KEY `idx_work_count_video_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
