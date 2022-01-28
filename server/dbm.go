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

	"gorm.io/gorm"
)

var (
	defaultLogLoad  *DefaultLogLoad
	defaultLogStore []*DefaultLogStore
	customRun       bool // 是否由用户自定义启动
)

type (
	IRunner interface {
		Run()
	}

	DefaultLogLoad struct {
		queue *datautil.Queue
		fn    func(tableName string, id uint64, num int) ([]*dblog_model.LogTable, error)
	}

	DefaultLogStore struct {
		queue *datautil.Queue
		fn    func(policy *dblog_model.Policy, data []*dblog_model.LogTable) error
	}
)

// InitRunner 提供runner
func SetCustom() {
	customRun = true
}

// Register 初始化db等 调用时的入口函数
func Register(db *gorm.DB, structHandler structhandler.IStructHandler, tables ...interface{}) error {
	core.Init(core.InitConfig(), core.InitDataDB(db), core.InitLogDB(), core.InitTaskQueue(), core.InitStructHandler(structHandler), core.InitService())
	errs := service.ServiceGroupApp.Dblog.Meta.InitApp(tables...)
	if len(errs) > 0 {
		for _, item := range errs {
			fmt.Println(item.Error())
		}
		return errors.New("初始化失败")
	}

	// 执行默认逻辑
	if !customRun {
		defaultLogLoad = newLogLoadRunner(global.G_LogTaskQueue)
		defaultLogStore = newLogStoreRunner(global.G_LogTaskQueue, 10)
		go defaultLogLoad.Run()
		for _, item := range defaultLogStore {
			go item.Run()
		}
	} else {
		fmt.Println("执行自定义逻辑")
	}

	return nil
}

// AddTable 动态表
func AddTable(table interface{}, fields []string, ignore []string) {
	service.ServiceGroupApp.Dblog.Meta.AddTable(table, fields, ignore)
}

// 每隔一段时间读取n条数据加入到任务队列中
func (r *DefaultLogLoad) Run() {
	var id uint64

	for {
		service.ServiceGroupApp.Dblog.Meta.GetAllPolicy().Range(func(key, value interface{}) bool {
			tableName := key.(string)
			policy := value.(*dblog_model.Policy)
			res, err := r.fn(tableName, id, global.G_CONFIG.DataLog.PerReadNum)
			if err != nil {
				fmt.Println("获取数据失败", err.Error())
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
				fmt.Println("删除数据失败:", err.Error())
				return true
			}

			if res[len(res)-1].ID > id {
				id = res[len(res)-1].ID
			}
			return true
		})
		time.Sleep(time.Minute * 10)
	}
}

func (r *DefaultLogStore) Run() {
	for {
		if task := r.queue.GetTask(); task != nil {
			payload := task.PayLoad.(map[string]interface{}) // data, policy
			data := payload["data"].([]*dblog_model.LogTable)
			policy := payload["policy"].(*dblog_model.Policy)
			if err := r.fn(policy, data); err != nil {
				fmt.Println("消费者执行错误", err.Error())
				task.SetResult(false)
			} else {
				task.SetResult(true)
			}
		}

		time.Sleep(10 * time.Second)
	}
}

// 生产者
func newLogLoadRunner(queue *datautil.Queue) *DefaultLogLoad {
	return &DefaultLogLoad{
		queue: queue,
		fn:    service.ServiceGroupApp.Dblog.Logger.Load(),
	}
}

// 消费者
func newLogStoreRunner(queue *datautil.Queue, num int) []*DefaultLogStore {
	rs := make([]*DefaultLogStore, num)

	for i := 0; i < num; i++ {
		rs[i] = &DefaultLogStore{
			queue: queue,
			fn:    service.ServiceGroupApp.Dblog.Logger.Store(),
		}
	}

	return rs
}
