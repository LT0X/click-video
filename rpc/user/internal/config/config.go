package config

import (
	"strings"

	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	DBList          DBListConf
	MetricsListenOn string
}

const defaultMetricsListenAddress = "127.0.0.1:9112"

// MetricsListenAddress 返回配置地址；未配置时限制在本机 loopback。
func (c Config) MetricsListenAddress() string {
	if address := strings.TrimSpace(c.MetricsListenOn); address != "" {
		return address
	}
	return defaultMetricsListenAddress
}

type DBListConf struct {
	Mysql MysqlConf
	Redis RedisConf
}

type MysqlConf struct {
	Address     string
	Username    string
	Password    string
	DBName      string
	TablePrefix string
}
type RedisConf struct {
	Host     string
	Port     string
	Password string
	PoolSize int
}
