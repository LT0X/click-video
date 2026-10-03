-- 现有数据库升级：新增 user.rpc 所有的点赞版本状态与事务 Outbox。
CREATE TABLE IF NOT EXISTS `favorite_count_outbox` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `event_id` bigint unsigned NOT NULL,
  `user_id` bigint unsigned NOT NULL,
  `author_id` bigint unsigned NOT NULL DEFAULT '0',
  `video_id` bigint unsigned NOT NULL,
  `count_version` bigint unsigned NOT NULL,
  `delta` bigint NOT NULL,
  `favorite_count` bigint NOT NULL,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `published_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_favorite_count_outbox_event` (`event_id`),
  UNIQUE KEY `idx_favorite_count_outbox_version` (`video_id`,`count_version`),
  KEY `idx_favorite_count_outbox_pending` (`published_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `favorite_video_count_state` (
  `video_id` bigint unsigned NOT NULL,
  `favorite_count` bigint NOT NULL DEFAULT '0',
  `count_version` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`video_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `favorite_action_state` (
  `user_id` bigint unsigned NOT NULL,
  `video_id` bigint unsigned NOT NULL,
  `last_issued_sequence` bigint unsigned NOT NULL DEFAULT '0',
  `last_action_sequence` bigint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`user_id`,`video_id`),
  KEY `idx_favorite_action_state_video_id` (`video_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO `favorite_video_count_state` (`video_id`, `favorite_count`, `count_version`)
SELECT `video_id`, COUNT(*), 0 FROM `favorite` GROUP BY `video_id`;
