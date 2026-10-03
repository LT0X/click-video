package upload

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// WriteMetadata 在任务目录中原子保存 init 信息，进程重启后仍可审计本地分片归属。
func (m *Manager) WriteMetadata(meta Metadata) error {
	if err := validateUploadID(meta.UploadID); err != nil {
		return err
	}
	if m.config.TempDir == "" {
		return errors.New("上传临时目录未配置")
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("编码上传元数据失败: %w", err)
	}
	dir := filepath.Join(m.config.TempDir, meta.UploadID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("创建上传临时目录失败: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".meta-*.tmp")
	if err != nil {
		return fmt.Errorf("创建上传元数据临时文件失败: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("写入上传元数据失败: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("同步上传元数据失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("关闭上传元数据失败: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(dir, "meta.json")); err != nil {
		return fmt.Errorf("发布上传元数据失败: %w", err)
	}
	return nil
}

// HasPart 只报告磁盘上的完整分片；Redis Set 中的过期成员不能让续传跳过缺失文件。
func (m *Manager) HasPart(uploadID string, partNumber int) (bool, error) {
	if err := validateUploadID(uploadID); err != nil {
		return false, err
	}
	if m.config.TempDir == "" {
		return false, errors.New("上传临时目录未配置")
	}
	if partNumber < 1 || partNumber > m.config.MaxParts {
		return false, errInvalidPart
	}
	path := filepath.Join(m.config.TempDir, uploadID, fmt.Sprintf("part_%05d", partNumber))
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("读取分片状态失败: %w", err)
	}
	return info.Mode().IsRegular(), nil
}

// CleanupExpired 清除本地临时目录中超过 TTL 的合法上传 ID 目录。
func (m *Manager) CleanupExpired(now time.Time, ttl time.Duration) ([]string, error) {
	if m.config.TempDir == "" {
		return nil, errors.New("上传临时目录未配置")
	}
	if ttl <= 0 {
		return nil, errors.New("上传清理 TTL 必须大于 0")
	}
	entries, err := os.ReadDir(m.config.TempDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("扫描上传临时目录失败: %w", err)
	}

	removed := make([]string, 0)
	for _, entry := range entries {
		if !entry.IsDir() || validateUploadID(entry.Name()) != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return removed, fmt.Errorf("读取上传目录信息失败: %w", err)
		}
		if age := now.Sub(info.ModTime()); age < ttl {
			continue
		}
		if err := os.RemoveAll(filepath.Join(m.config.TempDir, entry.Name())); err != nil {
			return removed, fmt.Errorf("清理过期上传目录失败: %w", err)
		}
		removed = append(removed, entry.Name())
	}
	sort.Strings(removed)
	return removed, nil
}

// UploadIDs 返回临时目录下的合法上传 ID，供清理任务逐个确认 Redis 状态。
func (m *Manager) UploadIDs() ([]string, error) {
	if m.config.TempDir == "" {
		return nil, errors.New("上传临时目录未配置")
	}
	entries, err := os.ReadDir(m.config.TempDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("扫描上传临时目录失败: %w", err)
	}
	ids := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() && validateUploadID(entry.Name()) == nil {
			ids = append(ids, entry.Name())
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// RemoveUpload 删除指定任务目录；调用前必须由状态服务确认任务过期或不存在。
func (m *Manager) RemoveUpload(uploadID string) error {
	if err := validateUploadID(uploadID); err != nil {
		return err
	}
	if m.config.TempDir == "" {
		return errors.New("上传临时目录未配置")
	}
	if err := os.RemoveAll(filepath.Join(m.config.TempDir, uploadID)); err != nil {
		return fmt.Errorf("删除上传临时目录失败: %w", err)
	}
	return nil
}

// UploadExpired 判断单个合法任务目录是否超过生命周期，供状态服务确认 Redis 已失效后清理。
func (m *Manager) UploadExpired(uploadID string, now time.Time, ttl time.Duration) (bool, error) {
	if err := validateUploadID(uploadID); err != nil {
		return false, err
	}
	if m.config.TempDir == "" {
		return false, errors.New("上传临时目录未配置")
	}
	if ttl <= 0 {
		return false, errors.New("上传清理 TTL 必须大于 0")
	}
	info, err := os.Stat(filepath.Join(m.config.TempDir, uploadID))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("读取上传目录信息失败: %w", err)
	}
	return now.Sub(info.ModTime()) >= ttl, nil
}
