package upload

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gofrs/uuid"
)

const (
	defaultPartSize         int64 = 5 * 1024 * 1024
	defaultMaxChunkSize     int64 = 10 * 1024 * 1024
	defaultMaxParts               = 10000
	defaultMergeConcurrency       = 3
	mergeBufferSize               = 256 * 1024
	copyBufferSize                = 32 * 1024
)

var (
	errInvalidUploadID = errors.New("上传 ID 不合法")
	errInvalidPart     = errors.New("分片序号或分片数量不合法")
	errInvalidSize     = errors.New("文件大小不合法")
	errPartConflict    = errors.New("分片已存在且内容不同")
)

// Config 描述本地分片目录、正式视频目录和上传容量限制。
type Config struct {
	TempDir          string
	VideoDir         string
	PartSize         int64
	MaxChunkSize     int64
	MaxUploadSize    int64
	MaxParts         int
	MergeConcurrency int
}

// Manager 只管理本地文件；Redis 状态和视频元数据由上层服务负责。
type Manager struct {
	config     Config
	mergeSlots chan struct{}
	mergeLocks [64]sync.Mutex
}

// NewManager 使用安全默认值补齐可选的容量配置。
func NewManager(config Config) *Manager {
	if config.PartSize <= 0 {
		config.PartSize = defaultPartSize
	}
	if config.MaxChunkSize <= 0 {
		config.MaxChunkSize = defaultMaxChunkSize
	}
	if config.MaxChunkSize < config.PartSize {
		config.MaxChunkSize = config.PartSize
	}
	if config.MaxParts <= 0 {
		config.MaxParts = defaultMaxParts
	}
	if config.MaxUploadSize <= 0 {
		config.MaxUploadSize = config.PartSize * int64(config.MaxParts)
	}
	if config.MergeConcurrency <= 0 {
		config.MergeConcurrency = defaultMergeConcurrency
	}
	return &Manager{
		config:     config,
		mergeSlots: make(chan struct{}, config.MergeConcurrency),
	}
}

// WritePart 以固定缓冲区把单片写入临时文件，校验完整后再原子发布分片文件。
func (m *Manager) WritePart(uploadID string, partNumber, totalParts int, src io.Reader, declaredSize int64) (string, error) {
	if err := validateUploadID(uploadID); err != nil {
		return "", err
	}
	if totalParts < 1 || totalParts > m.config.MaxParts || partNumber < 1 || partNumber > totalParts {
		return "", errInvalidPart
	}
	if src == nil || declaredSize <= 0 || declaredSize > m.config.MaxChunkSize {
		return "", errInvalidSize
	}
	if m.config.TempDir == "" {
		return "", errors.New("上传临时目录未配置")
	}

	dir := filepath.Join(m.config.TempDir, uploadID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", fmt.Errorf("创建上传临时目录失败: %w", err)
	}
	tmp, err := os.CreateTemp(dir, fmt.Sprintf(".part_%05d-*.tmp", partNumber))
	if err != nil {
		return "", fmt.Errorf("创建分片临时文件失败: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	digest := md5.New()
	// 上限多读 1 字节，拒绝伪造 Content-Length 或超限的 chunked 请求。
	written, copyErr := io.Copy(io.MultiWriter(tmp, digest), io.LimitReader(src, m.config.MaxChunkSize+1))
	if copyErr != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("写入视频分片失败: %w", copyErr)
	}
	if written != declaredSize || written > m.config.MaxChunkSize {
		_ = tmp.Close()
		return "", fmt.Errorf("分片实际大小 %d 与声明大小 %d 不符", written, declaredSize)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return "", fmt.Errorf("同步分片文件失败: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("关闭分片文件失败: %w", err)
	}

	partPath := filepath.Join(dir, fmt.Sprintf("part_%05d", partNumber))
	if err := os.Link(tmpPath, partPath); err != nil {
		if !os.IsExist(err) {
			return "", fmt.Errorf("发布分片文件失败: %w", err)
		}
		// 并发重试可能先一步写入同一分片；只接受字节完全相同的重试。
		existingMD5, existingSize, hashErr := fileMD5(partPath)
		if hashErr != nil {
			return "", fmt.Errorf("校验已存在分片失败: %w", hashErr)
		}
		if existingSize != written || existingMD5 != hex.EncodeToString(digest.Sum(nil)) {
			return "", errPartConflict
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// Merge 按序合并分片，在同一遍流中校验总长度和 MD5 后才发布正式文件。
func (m *Manager) Merge(uploadID string, totalParts int, expectedSize int64, expectedMD5 string) (string, error) {
	if err := validateUploadID(uploadID); err != nil {
		return "", err
	}
	if totalParts < 1 || totalParts > m.config.MaxParts {
		return "", errInvalidPart
	}
	if expectedSize <= 0 || expectedSize > m.config.MaxUploadSize {
		return "", errInvalidSize
	}
	if !isMD5(expectedMD5) {
		return "", errors.New("文件 MD5 格式不合法")
	}
	if m.config.VideoDir == "" {
		return "", errors.New("正式视频目录未配置")
	}

	if err := os.MkdirAll(m.config.VideoDir, 0o750); err != nil {
		return "", fmt.Errorf("创建视频目录失败: %w", err)
	}
	finalPath := filepath.Join(m.config.VideoDir, uploadID+".mp4")
	lock := &m.mergeLocks[uuidHash(uploadID)%uint32(len(m.mergeLocks))]
	lock.Lock()
	defer lock.Unlock()

	// 视频记录 RPC 失败时允许客户端重试 merge；已验证的正式文件可直接复用。
	if digest, size, err := fileMD5(finalPath); err == nil {
		if size == expectedSize && strings.EqualFold(digest, expectedMD5) {
			return finalPath, nil
		}
		return finalPath, errors.New("已存在的合并文件与当前上传校验信息不符")
	} else if !os.IsNotExist(err) {
		return finalPath, fmt.Errorf("检查正式视频文件失败: %w", err)
	}

	err := m.withMergeSlot(func() error {
		return m.mergeToFile(uploadID, totalParts, expectedSize, expectedMD5, finalPath)
	})
	return finalPath, err
}

func (m *Manager) mergeToFile(uploadID string, totalParts int, expectedSize int64, expectedMD5, finalPath string) error {
	partDir := filepath.Join(m.config.TempDir, uploadID)
	output, err := os.CreateTemp(m.config.VideoDir, "."+uploadID+"-*.merge")
	if err != nil {
		return fmt.Errorf("创建合并临时文件失败: %w", err)
	}
	tmpPath := output.Name()
	keep := false
	defer func() {
		_ = output.Close()
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()

	buffered := bufio.NewWriterSize(output, mergeBufferSize)
	digest := md5.New()
	writer := io.MultiWriter(buffered, digest)
	copyBuffer := make([]byte, copyBufferSize)
	var totalWritten int64
	for partNumber := 1; partNumber <= totalParts; partNumber++ {
		partPath := filepath.Join(partDir, fmt.Sprintf("part_%05d", partNumber))
		part, err := os.Open(partPath)
		if err != nil {
			return fmt.Errorf("打开第 %d 个分片失败: %w", partNumber, err)
		}
		remaining := expectedSize - totalWritten
		written, copyErr := io.CopyBuffer(writer, io.LimitReader(bufio.NewReaderSize(part, mergeBufferSize), remaining+1), copyBuffer)
		closeErr := part.Close()
		if copyErr != nil {
			return fmt.Errorf("合并第 %d 个分片失败: %w", partNumber, copyErr)
		}
		if closeErr != nil {
			return fmt.Errorf("关闭第 %d 个分片失败: %w", partNumber, closeErr)
		}
		totalWritten += written
		if totalWritten > expectedSize {
			return errors.New("合并后文件大小超过声明值")
		}
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("刷新合并缓冲区失败: %w", err)
	}
	if totalWritten != expectedSize {
		return fmt.Errorf("合并后文件大小 %d 与声明大小 %d 不符", totalWritten, expectedSize)
	}
	if got := hex.EncodeToString(digest.Sum(nil)); !strings.EqualFold(got, expectedMD5) {
		return errors.New("合并后文件 MD5 校验失败")
	}
	if err := output.Chmod(0o644); err != nil {
		return fmt.Errorf("设置正式视频文件权限失败: %w", err)
	}
	if err := output.Sync(); err != nil {
		return fmt.Errorf("同步合并文件失败: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("关闭合并文件失败: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		// 另一请求可能已通过 Redis 外部锁之外的调用发布了相同结果。
		digest, size, checkErr := fileMD5(finalPath)
		if checkErr == nil && size == expectedSize && strings.EqualFold(digest, expectedMD5) {
			return nil
		}
		return fmt.Errorf("发布合并视频失败: %w", err)
	}
	keep = true
	return nil
}

func (m *Manager) withMergeSlot(run func() error) error {
	m.mergeSlots <- struct{}{}
	defer func() { <-m.mergeSlots }()
	return run()
}

func validateUploadID(uploadID string) error {
	if _, err := uuid.FromString(uploadID); err != nil {
		return errInvalidUploadID
	}
	return nil
}

func isMD5(value string) bool {
	if len(value) != md5.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func fileMD5(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	digest := md5.New()
	size, err := io.Copy(digest, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(digest.Sum(nil)), size, nil
}

func uuidHash(value string) uint32 {
	var sum uint32 = 2166136261
	for i := 0; i < len(value); i++ {
		sum ^= uint32(value[i])
		sum *= 16777619
	}
	return sum
}
