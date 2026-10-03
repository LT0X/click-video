package util

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/disintegration/imaging"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"go.uber.org/zap"
)

func GetSnapshot(videoPath, imagePath string, frameNum int) (string, error) {
	if videoPath == "" || imagePath == "" || frameNum < 0 {
		return "", fmt.Errorf("视频或封面路径不合法")
	}
	buf := bytes.NewBuffer(nil)
	err := ffmpeg.Input(videoPath).Filter("select", ffmpeg.Args{fmt.Sprintf("gte(n,%d)", frameNum)}).
		Output("pipe:", ffmpeg.KwArgs{"vframes": 1, "format": "image2", "vcodec": "mjpeg"}).
		WithOutput(buf, os.Stdout).
		Run()

	if err != nil {
		zap.L().Error("生成缩略图失败：" + err.Error())
		return "", err
	}

	img, err := imaging.Decode(buf)
	if err != nil {
		zap.L().Error("生成缩略图失败：" + err.Error())
		return "", err
	}

	if err := os.MkdirAll(filepath.Dir(imagePath), 0o750); err != nil {
		return "", fmt.Errorf("创建封面目录失败: %w", err)
	}
	if err := imaging.Save(img, imagePath); err != nil {
		return "", fmt.Errorf("保存视频封面失败: %w", err)
	}
	return imagePath, nil
}
