package server

import (
	"errors"
	"fmt"
	"time"

	"datamanager/server/common/datautil"
	"datamanager/server/common/structhandler"
	"datamanager/server/core"
	"datamanager/server/global"
	dblog_model "datamanager/server/model/dblog"
	"datamanager/server/service"

	"datamanager/server/base"

	"gorm.io/gorm"
)

const nums = 1000 // 默认获取日志条数

var (
	initDB bool // 是否初始化了数据库
)

type (
	// 后台服务
	dataManagerService struct {
		base.Service
	}

	// 日志的生产者
	LogLoadRunner struct {
		base.Runner
		queue *datautil.Queue
		fn    func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error)
	}

	// 日志的消费者
	LogStoreRunner struct {
		base.Runner
		queue *datautil.Queue
		fn    func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error
	}
)

// InitService 初始化后台服务 入口函数 可以空跑 之后再初始化数据库连接
func InitService(db *gorm.DB) {
	core.Init(core.InitConfig(), core.InitDataDB(db), core.InitLogDB(), core.InitTaskQueue(), core.InitStructHandler(), core.InitService())
	runners := append([]base.IRunner{NewLogLoadRunner(global.G_LogTaskQueue)}, newLogStoreRunner(global.G_LogTaskQueue, 10)...)
	base.InitService(&dataManagerService{}, runners...)
}

// InitDB 初始化db 这里的表是强绑定的
// structHandler 回调接口 提供获取表列表 表名字的回调
func InitDB(structHandler structhandler.IStructHandler, tables ...interface{}) error {
	errs := service.ServiceGroupApp.Dblog.Meta.InitApp(global.G_StructHandler, tables...)
	if len(errs) > 0 {
		for _, item := range errs {
			fmt.Println(item.Error())
		}
		return errors.New("初始化失败")
	}

	initDB = true
	return nil
}

func AddTable(table interface{}, fields []string, ignore []string) {
	service.ServiceGroupApp.Dblog.Meta.AddTable(table, fields, ignore)
}

// 生产者

func NewLogLoadRunner(queue *datautil.Queue) *LogLoadRunner {
	return &LogLoadRunner{
		queue: queue,
		fn:    service.ServiceGroupApp.Dblog.Logger.Load(),
	}
}

// 每隔一段时间读取n条数据加入到任务队列中
func (r *LogLoadRunner) Run() {
	var id uint64

	for {
		if !initDB {
			r.Sleep(1 * time.Second)
			continue
		}
		service.ServiceGroupApp.Dblog.Meta.GetAllPolicy().Range(func(key, value interface{}) bool {
			tableName := key.(string)
			policy := value.(*dblog_model.Policy)
			res, err := r.fn(tableName, id, nums)
			if err != nil {
				r.Warn("获取数据失败", err.Error())
				return true
			}
			if len(res) == 0 {
				return true
			}

			if !<-r.queue.Push(map[string]interface{}{
				"data":   res,
				"policy": policy,
			}) {
				return true
			}
			if err := dblog_model.LogTableRepo.DeleteByID(res); err != nil {
				r.Warn("删除数据失败:", err.Error())
				return true
			}

			if res[len(res)-1].ID > id {
				id = res[len(res)-1].ID
			}
			return true
		})
		r.Sleep(time.Minute * 10)
	}
}

// 消费者

func newLogStoreRunner(queue *datautil.Queue, num int) []base.IRunner {
	rs := make([]base.IRunner, num)

	for i := 0; i < num; i++ {
		rs[i] = &LogStoreRunner{
			queue: queue,
			fn:    service.ServiceGroupApp.Dblog.Logger.Store(),
		}
	}

	return rs
}

func (r *LogStoreRunner) Run() {
	for {
		if !initDB {
			r.Sleep(1 * time.Second)
			continue
		}

		if task := r.queue.GetTask(); task != nil {
			payload := task.PayLoad.(map[string]interface{}) // data, policy
			data := payload["data"].([]*dblog_model.LogTable)
			policy := payload["policy"].(*dblog_model.Policy)
			if err := r.fn(policy, data); err != nil {
				r.Warn("消费者执行错误", err.Error())
				task.SetResult(false)
			} else {
				task.SetResult(true)
			}
		}

		r.Sleep(10 * time.Second)
	}
}
