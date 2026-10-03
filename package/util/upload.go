package util

import (
	"bytes"
	"context"
	"douyin/config"
	"github.com/gofrs/uuid"
	"os"
	"path/filepath"

	"github.com/disintegration/imaging"
	"github.com/qiniu/go-sdk/v7/auth/qbox"
	"github.com/qiniu/go-sdk/v7/storage"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"go.uber.org/zap"
)

func UploadVideo(file []byte, fileName string) (string, string, error) {
	playDir := filepath.Join(config.System.HttpAddress.VideoAddress, "playurl")
	err := os.MkdirAll(playDir, 0o750)
	if err != nil {
		zap.L().Error(err.Error())
		return "", "", err
	}
	outputFilePath := filepath.Join(playDir, fileName)
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		zap.L().Error(err.Error())
		return "", "", err
	}
	defer outputFile.Close()
	_, err = outputFile.Write(file)
	if err != nil {
		zap.L().Error(err.Error())
		return "", "", err
	}
	zap.L().Info(fileName + "已成功写入文件夹")
	//生成cover url;
	u1, err := uuid.NewV4()
	if err != nil {
		zap.L().Error(err.Error())
		return "", "", err
	}
	coverDir := filepath.Join(config.System.HttpAddress.VideoAddress, "coverurl")
	coverPath := filepath.Join(coverDir, u1.String()+".png")
	_, err = GetSnapshot(outputFilePath, coverPath, 1)
	if err != nil {
		zap.L().Error(err.Error())
		return "", "", err
	}
	coverUrl := "http://127.0.0.1:8000/static/coverurl/" + u1.String() + ".png"

	return fileName, coverUrl, nil
}

func UploadAvater(file []byte, fileName string) error {
	err := os.MkdirAll(config.System.HttpAddress.VideoAddress, os.ModePerm)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	// 还得有个变量是宿主机ip
	outputFilePath := filepath.Join("C:\\work\\GoWord\\v1-clip\\DikTok\\douyinVideo\\avater", fileName)
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	defer outputFile.Close()
	_, err = outputFile.Write(file)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	zap.L().Info(fileName + "已成功写入文件夹")

	return nil
}

// 使用ffmpeg deprecated 已弃用
func GetVideoFrame(outputFilePath, fileName string) error {
	// 提取视频的第一帧
	picBuffer := bytes.NewBuffer(nil)
	err := ffmpeg.Input(outputFilePath).
		Filter("select", ffmpeg.Args{"gte(n,1)"}).
		Output("pipe:", ffmpeg.KwArgs{"vframes": 1, "format": "image2", "vcodec": "mjpeg"}).
		WithOutput(picBuffer).
		Run()
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	img, err := imaging.Decode(picBuffer)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	// 创建图片文件夹
	// "" 替代 config.System.HttpAddress.ImageAddress
	err = os.MkdirAll("", os.ModePerm)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	// 保存
	imagPath := fileName + ".jpg"
	err = imaging.Save(img, imagPath)
	if err != nil {
		zap.L().Error(err.Error())
		return nil
	}
	return nil
}

func UploadToOSS(fileName, filePath string) error {
	putPolicy := storage.PutPolicy{
		Scope: config.System.Qiniu.Bucket,
	}
	mac := qbox.NewMac(config.System.Qiniu.AccessKey, config.System.Qiniu.SecretKey)
	upToken := putPolicy.UploadToken(mac)

	cfg := storage.Config{}
	// 是否使用https域名
	cfg.UseHTTPS = true
	// 上传是否使用CDN上传加速
	cfg.UseCdnDomains = false

	// 构建表单上传的对象
	formUploader := storage.NewFormUploader(&cfg)
	ret := storage.PutRet{}

	// 可选配置
	putExtra := storage.PutExtra{
		Params: map[string]string{
			"x:name": "github logo",
		},
	}
	// 这里其实耗时很久 感觉有3/4秒
	err := formUploader.PutFile(context.Background(), &ret, upToken, fileName, filePath, &putExtra)
	if err != nil {
		zap.L().Error(err.Error())
		return err
	}
	return nil
}
