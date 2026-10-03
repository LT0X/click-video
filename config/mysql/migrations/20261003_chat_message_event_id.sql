-- 消息由 Redis Stream 至少一次投递，事件 ID 唯一约束保证重试不会重复写入。
ALTER TABLE `message` ADD COLUMN `event_id` varchar(64) DEFAULT NULL;
CREATE UNIQUE INDEX `uk_message_event_id` ON `message` (`event_id`);
