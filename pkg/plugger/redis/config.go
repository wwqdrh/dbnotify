package redis

import (
	"fmt"

	"github.com/go-redis/redis/v8"
)

type RedisConfig struct {
	Host     string
	Port     int
	UserName string
	Password string
	DB       int // 0 default DB
	// TLSConfig
}

// RedisConnPool redis连接池管理
type RedisConnPool struct {
}

// NewRedisConfig redisconfig构造函数 必须有的参数是host、port、db
func NewRedisConfig(host string, port int, db int) *RedisConfig {
	return &RedisConfig{
		Host: host,
		Port: port,
		DB:   db,
	}
}

func (c *RedisConfig) SetUserName(value string) *RedisConfig {
	c.UserName = value
	return c
}

func (c *RedisConfig) SetPassword(value string) *RedisConfig {
	c.Password = value
	return c
}

func (c RedisConfig) DSN() interface{} {
	return &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Password: c.Password,
		DB:       c.DB, // default DB,
		// TLSConfig: &tls.Config{},
	}
}
