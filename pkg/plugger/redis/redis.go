package redis

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

// 配置 测试连接 数据初始化

type RedisDriver struct {
	config *RedisConfig
	Client *redis.Client
}

// NewRedisDriver driver构造函数
func NewRedisDriver(config *RedisConfig) (*RedisDriver, error) {
	driver := &RedisDriver{
		config: config,
	}
	if !driver.Ping() {
		return nil, errors.New("redis 连接失败")
	}
	return driver, nil
}

// Ping 测试连通性
func (d *RedisDriver) Ping() bool {
	client := redis.NewClient(d.config.DSN().(*redis.Options))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	return err == nil
}

// Initalial 连接
func (d *RedisDriver) Initalial() error {
	client := redis.NewClient(d.config.DSN().(*redis.Options))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	d.Client = client
	return nil
}

// 从连接池中获取连接 redis内部也同样维护了连接池
func (d *RedisDriver) Connection() interface{} {
	return d.Client
}
