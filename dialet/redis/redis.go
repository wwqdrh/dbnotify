package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

var (
	ErrIdle   = errors.New("connection error")
	PolicyKey = "watch:policy:key"
)

type RedisDialet struct {
	client *goredis.Client
	policy []string
}

func NewRedisDialet(endpoint, password string) (*RedisDialet, error) {
	dialet := &RedisDialet{
		client: goredis.NewClient(&goredis.Options{
			Addr:     endpoint,
			Password: password,
			DB:       0,
			// TLSConfig: &tls.Config{},
		}),
	}

	if err := dialet.Ping(); err != nil {
		return nil, err
	} else {
		return dialet, nil
	}
}

func (d *RedisDialet) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := d.client.Ping(ctx).Result()
	if err != nil {
		return ErrIdle
	} else {
		return nil
	}
}

// 获取配置列表
// 存储在redis中，使用list存储
func (d *RedisDialet) Initial() error {
	result, err := d.client.LRange(context.TODO(), PolicyKey, 0, -1).Result()
	if err != nil {
		return err
	}
	d.policy = result
	return nil
}

func (d *RedisDialet) ModifyPolicy() error {
	return nil
}

func (d *RedisDialet) AddPolicy(policy string) error {
	_, err := d.client.LPush(context.TODO(), PolicyKey, policy).Result()
	if err != nil {
		return err
	}
	d.policy = append(d.policy, policy)
	return nil
}

// 触发更新
func (d *RedisDialet) ListPolicy() error {
	result, err := d.client.LRange(context.TODO(), PolicyKey, 0, -1).Result()
	if err != nil {
		return err
	}
	d.policy = result
	return nil
}

func (d *RedisDialet) DeletePolicy() error { return nil }

func (d *RedisDialet) Watch(ctx context.Context) chan interface{} { return nil }
