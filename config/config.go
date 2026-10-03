package config

import (
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type MySQL struct {
	Host        string `mapstructure:"host"`
	Port        string `mapstructure:"port"`
	UserName    string `mapstructure:"username"`
	Password    string `mapstructure:"password"`
	Database    string `mapstructure:"database"`
	MaxOpenConn int    `mapstructure:"maxOpenConn"`
	MaxIdleConn int    `mapstructure:"maxIdleConn"`
}

type HTTP struct {
	Host         string `mapstructure:"host"`
	Port         string `mapstructure:"port"`
	VideoAddress string `mapstructure:"videoAddress"`
}

type Redis struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Database int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"poolSize"`
	Password string `mapstructure:"password"`
}

type UploadConfig struct {
	TempDir                string `mapstructure:"tempDir"`
	PublicBaseURL          string `mapstructure:"publicBaseURL"`
	PartSize               int64  `mapstructure:"partSize"`
	MaxChunkSize           int64  `mapstructure:"maxChunkSize"`
	MaxUploadSize          int64  `mapstructure:"maxUploadSize"`
	MaxParts               int    `mapstructure:"maxParts"`
	MergeConcurrency       int    `mapstructure:"mergeConcurrency"`
	TTLHours               int    `mapstructure:"ttlHours"`
	TTLJitterSeconds       int    `mapstructure:"ttlJitterSeconds"`
	CleanupIntervalMinutes int    `mapstructure:"cleanupIntervalMinutes"`
}

type RabbitMQ struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
}

type QiNiuCloud struct {
	Bucket    string `mapstructure:"bucket"`
	AccessKey string `mapstructure:"accessKey"`
	SecretKey string `mapstructure:"secretKey"`
	OssDomain string `mapstructure:"ossDomain"`
}

type SystemConfig struct {
	Qiniu        QiNiuCloud   `mapstructure:"qiniu"`
	HttpAddress  HTTP         `mapstructure:"httpAddress"`
	MysqlMaster  MySQL        `mapstructure:"mysqlMaster"`
	MysqlSlave   MySQL        `mapstructure:"mysqlSlave"`
	UserRedis    Redis        `mapstructure:"userRedis"`
	VideoRedis   Redis        `mapstructure:"videoRedis"`
	CommentRedis Redis        `mapstructure:"commentRedis"`
	Upload       UploadConfig `mapstructure:"upload"`
	MQ           RabbitMQ     `mapstructure:"rabbitmq"`
	Mode         string       `mapstructure:"mode"`
	JwtSecret    string       `mapstructure:"jwtSecret"`
	GPTSecret    string       `mapstructure:"gptSecret"`
}

var System SystemConfig

// LoadRabbitMQ 让独立 RPC 进程复用网关的 RabbitMQ 配置，不在各服务配置中复制凭据。
func LoadRabbitMQ() (RabbitMQ, error) {
	paths := []string{os.Getenv("CLICK_VIDEO_CONFIG"), filepath.Join("config", "config.yaml"), filepath.Join("..", "..", "config", "config.yaml")}
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			continue
		}
		loader := viper.New()
		loader.SetConfigFile(path)
		if err := loader.ReadInConfig(); err != nil {
			return RabbitMQ{}, errors.New("读取 RabbitMQ 配置文件失败")
		}
		var rabbitMQ RabbitMQ
		if err := loader.UnmarshalKey("rabbitmq", &rabbitMQ); err != nil {
			return RabbitMQ{}, errors.New("解析 RabbitMQ 配置失败")
		}
		if rabbitMQ.Host == "" || rabbitMQ.Port == "" || rabbitMQ.User == "" {
			return RabbitMQ{}, errors.New("RabbitMQ 配置缺少 host、port 或 user")
		}
		return rabbitMQ, nil
	}
	return RabbitMQ{}, errors.New("未找到 config/config.yaml；可通过 CLICK_VIDEO_CONFIG 指定")
}

func Init() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./config/")

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal("fatal error config file: ", err.Error())
	}

	err = viper.Unmarshal(&System)
	if err != nil {
		log.Fatal("fatal error unmarshal config: ", err.Error())
	}
	applyUploadDefaults()

	// 监视配置文件的变化 有变化就更改
	viper.WatchConfig()
	viper.OnConfigChange(func(in fsnotify.Event) {
		log.Println("配置文件被修改 重新载入全局变量")
		err = viper.Unmarshal(&System)
		if err != nil {
			log.Println("fatal error unmarshal config: ", err.Error())
		}
		applyUploadDefaults()
		log.Println(System.Qiniu.OssDomain)
	})
	log.Println("viper读取配置文件成功")
}

func applyUploadDefaults() {
	if System.Upload.TempDir == "" {
		System.Upload.TempDir = filepath.Join(System.HttpAddress.VideoAddress, "upload", "tmp")
	}
	if System.Upload.PartSize <= 0 {
		System.Upload.PartSize = 5 * 1024 * 1024
	}
	if System.Upload.MaxChunkSize <= 0 || System.Upload.MaxChunkSize < System.Upload.PartSize {
		System.Upload.MaxChunkSize = 10 * 1024 * 1024
		if System.Upload.MaxChunkSize < System.Upload.PartSize {
			System.Upload.MaxChunkSize = System.Upload.PartSize
		}
	}
	if System.Upload.MaxParts <= 0 || System.Upload.MaxParts > 10000 {
		System.Upload.MaxParts = 10000
	}
	if System.Upload.MaxUploadSize <= 0 {
		System.Upload.MaxUploadSize = System.Upload.PartSize * int64(System.Upload.MaxParts)
	}
	if System.Upload.MergeConcurrency <= 0 || System.Upload.MergeConcurrency > 3 {
		System.Upload.MergeConcurrency = 3
	}
	if System.Upload.TTLHours <= 0 {
		System.Upload.TTLHours = 24
	}
	if System.Upload.TTLJitterSeconds <= 0 {
		System.Upload.TTLJitterSeconds = 300
	}
	if System.Upload.CleanupIntervalMinutes <= 0 {
		System.Upload.CleanupIntervalMinutes = 30
	}
}
