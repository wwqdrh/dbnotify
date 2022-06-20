package datamanager

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/wwqdrh/datamanager/dialet"
)

// cache的repo操作

type Fn func() interface{}

// 触发监听的策略
type Policy struct {
	Key   string // must unique
	Table string
	Field string // "*":全部 "a,b,c,d":指定字段
	Call  Fn
}

type cacheMap struct {
	sync.Map
	ch chan dialet.ILogData
}

// var cacheMap = sync.Map{} // key => cacheRepo

type cacheRepo struct {
	fn    *Policy
	Value interface{}
	wait  *list.List
}

func NewCacheMap(ch chan dialet.ILogData, ctx context.Context) *cacheMap {
	r := &cacheMap{
		Map: sync.Map{},
		ch:  ch,
	}

	go func() {
		select {
		case item := <-ch:
			r.Trigger(item)
		case <-ctx.Done():
			return
		}
	}()

	return r
}

// 等待事件 将当前的操作添加到cacheRepo的wait链表中 并发不安全
func (c *cacheMap) Wait(key string) {

}

func (c *cacheMap) Register(policy *Policy) {

}

func (c *cacheMap) Trigger(val dialet.ILogData) {

}

type Repo struct {
	CacheFn  sync.Map // map[string]*Policy     //
	ValueMap sync.Map // map[string]interface{} //
	WaitMap  map[string]*sync.Cond
	Chan     chan dialet.ILogData
	client   *redis.Client

	onceFlag uint32   // 用于实现安全的双检锁
	onceMap  sync.Map //  map[string]*keyRepo // 使用map映射
	lock     sync.Mutex
}

type keyRepo struct {
	once uint32
}

func NewRepo(ch chan dialet.ILogData) *Repo {
	return &Repo{
		CacheFn:  sync.Map{}, // map[string]*Policy{},
		ValueMap: sync.Map{}, //map[string]interface{}{},
		WaitMap:  map[string]*sync.Cond{},
		Chan:     ch,
		onceFlag: 0,
		onceMap:  sync.Map{},
		lock:     sync.Mutex{},
	}
}

// 配置redis客户端作为缓存工具
func (r *Repo) InitRedisCache(client *redis.Client) {
	r.client = client
}

func (r *Repo) GetValue(key string) interface{} {
	// if val, ok := r.ValueMap[key]; ok {
	if val, ok := r.ValueMap.Load(key); ok {
		return val
	}

	// if fn, ok := r.CacheFn[key]; !ok {
	if fn, ok := r.CacheFn.Load(key); !ok {
		return nil
	} else {
		v := fn.(*Policy).Call()
		// r.ValueMap[key] = v
		r.ValueMap.Store(key, v)
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

// 注册key以及处理函数(返回数据，用于更新缓存中的key)
func (r *Repo) Register(policy *Policy) {
	// r.CacheFn[policy.Key] = policy
	r.CacheFn.Store(policy.Key, policy)
}

func (r *Repo) GenInstance(key string) {
	if val, _ := r.onceMap.LoadOrStore(key, &keyRepo{}); atomic.LoadUint32(&val.(*keyRepo).once) == 0 {
		r.lock.Lock()
		if v := val.(*keyRepo); v.once == 0 {
			r.WaitMap[key] = sync.NewCond(&sync.Mutex{})
			atomic.StoreUint32(&v.once, 1)
		}
		r.lock.Unlock()
	}
}

// 触发key相应的更新操作
func (r *Repo) Trigger(log dialet.ILogData) {
	r.CacheFn.Range(func(k, value interface{}) bool {
		key := k.(string)
		policy := value.(*Policy)
		if policy.Table != log.GetTable() {
			return true
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
				return true
			}
		}

		// 验证通过，触发缓存执行
		// if val, ok := r.CacheFn[key]; ok {
		if val, ok := r.CacheFn.Load(key); ok {
			v := val.(*Policy).Call()
			// v := val.Call()
			// r.ValueMap[key] = v
			r.ValueMap.Store(key, v)

			if r.client != nil {
				r.SetValueV2(key, fmt.Sprint(v))
			}

			r.GenInstance(key)
			r.WaitMap[key].Broadcast()
		}
		return true
	})
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

// wait a cache trigger update
// 存在多个channel进行调用的情况
func (r *Repo) Wait(key string) {
	r.GenInstance(key)

	r.WaitMap[key].L.Lock() // wired 需要先加锁
	r.WaitMap[key].Wait()
	r.WaitMap[key].L.Unlock()
}
