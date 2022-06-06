package datamanager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/wwqdrh/datamanager/dialet"
)

// 触发监听的策略
type Policy struct {
	Key   string // must unique
	Table string
	Field string // "*":全部 "a,b,c,d":指定字段
	Call  Fn
}

// cache的repo操作

type Fn func() interface{}

type Repo struct {
	CacheFn  map[string]*Policy
	ValueMap map[string]interface{}
	Chan     chan dialet.ILogData
	client   *redis.Client
}

func NewRepo(ch chan dialet.ILogData) *Repo {
	return &Repo{
		CacheFn:  map[string]*Policy{},
		ValueMap: map[string]interface{}{},
		Chan:     ch,
	}
}

// 配置redis客户端作为缓存工具
func (r *Repo) InitRedisCache(client *redis.Client) {
	r.client = client
}

func (r *Repo) GetValue(key string) interface{} {
	if val, ok := r.ValueMap[key]; ok {
		return val
	}

	if fn, ok := r.CacheFn[key]; !ok {
		return nil
	} else {
		v := fn.Call()
		r.ValueMap[key] = v
		return v
	}
}

// 如果配置了redis就从redis中获取数据，否则从本地缓存中获取数据，
func (r *Repo) GetValueV2(key string) interface{} {
	if r.client == nil {
		return r.GetValue(key)
	}

	var res interface{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	val, err := r.client.Get(ctx, key).Result()
	switch {
	case err == redis.Nil:
		res = nil
	case err != nil:
		res = nil
	default:
		res = val
	}

	if res == nil {
		res = r.GetValue(key)
		r.SetValueV2(key, fmt.Sprint(res))
	}
	return res
}

func (r *Repo) SetValueV2(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := r.client.Set(ctx, key, value, 5*time.Second).Err()
	if err != nil {
		return err
	}
	return nil
}

// cache数据的初始化操作
// func (r *Repo) Initial() {

// }

// 注册key以及处理函数(返回数据，用于更新缓存中的key)
func (r *Repo) Register(policy *Policy) {
	r.CacheFn[policy.Key] = policy
}

// 触发key相应的更新操作
func (r *Repo) Trigger(log dialet.ILogData) {
	for key, policy := range r.CacheFn {
		if policy.Table != log.GetTable() {
			continue
		}

		if policy.Field != "*" {
			invalid := true
			for key := range log.GetPaylod() {
				if strings.Contains(policy.Field, key) {
					invalid = false
					break
				}
			}
			if invalid {
				continue
			}
		}

		// 验证通过，触发缓存执行
		if val, ok := r.CacheFn[key]; ok {
			v := val.Call()
			r.ValueMap[key] = v

			if r.client != nil {
				r.SetValueV2(key, fmt.Sprint(v))
			}
		}
	}
}

// 后台线程 获取操作日志
// !!!映射到注册的key(重点，考虑如何映射)
// 1、一张表对应多种缓存函数
// 2、更新了某一条记录之后，需要去判断出这条记录如何映射到缓存函数中
// 操作记录: table、changes、payload；缓存函数：表名、
// 这样才知道是更新哪一块缓存
func (r *Repo) Notify(ctx context.Context) {
	for {
		select {
		case item := <-r.Chan:
			r.Trigger(item)
		case <-ctx.Done():
			return
		}
	}
}
