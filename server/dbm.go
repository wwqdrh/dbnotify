package server

import (
	"errors"
	"fmt"
	"time"

	"datamanager/server/common"
	"datamanager/server/common/datautil"
	"datamanager/server/core"
	"datamanager/server/global"
	dblog_model "datamanager/server/model/dblog"
	"datamanager/server/service/dblog"
	"datamanager/server/types"

	"datamanager/server/base"

	"gorm.io/gorm"
)

const nums = 1000 // 默认获取日志条数

var (
	taskQueue *datautil.Queue
	initDB    bool // 是否初始化了数据库
	Handler   types.IStructHandler
)

type (
	// 后台服务
	dataManagerService struct {
		base.Service
	}

	// 动态字段名转换
	BaseStructHandler struct {
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
func InitService() {
	core.Init(core.InitConfig(), core.InitLogDB(), core.InitTaskQueue())
}

// InitDB 初始化db 这里的表是强绑定的
// structHandler 回调接口 提供获取表列表 表名字的回调
func InitDB(db *gorm.DB, logPath string, structHandler types.IStructHandler, tables ...interface{}) error {
	core.Init(core.InitDataDB(db))
	if structHandler == nil {
		structHandler = new(BaseStructHandler)
	}
	errs := common.InitApp(structHandler, tables...)
	if len(errs) > 0 {
		for _, item := range errs {
			fmt.Println(item.Error())
		}
		return errors.New("初始化失败")
	}
	runners := append([]base.IRunner{NewLogLoadRunner(global.G_LogTaskQueue)}, newLogStoreRunner(taskQueue, 10)...)
	base.InitService(&dataManagerService{}, runners...)

	initDB = true
	return nil
}

func AddTable(table interface{}, fields []string, ignore []string) {
	common.AddTable(table, fields, ignore)
}

// 默认的动态字段转换器

func (*BaseStructHandler) GetTables() []*types.Table {
	if global.G_DATADB == nil {
		return nil
	}
	tables := global.G_DATADB.ListTable()
	for _, item := range tables {
		if item.TableName == "" {
			item.TableName = item.TableID
		}
	}
	return tables
}

func (*BaseStructHandler) GetFields(tableName string) []*types.Fields {
	if global.G_DATADB == nil {
		return nil
	}

	fields := global.G_DATADB.ListTableField(tableName)
	for _, field := range fields {
		if field.FieldName == "" {
			field.FieldName = field.FieldID
		}
	}
	return fields
}

func (*BaseStructHandler) GetTableName(tableID string) string {
	return tableID
}

func (*BaseStructHandler) GetFieldName(tableID string, fieldID string) string {
	return fieldID
}

// 生产者

func NewLogLoadRunner(queue *datautil.Queue) *LogLoadRunner {
	return &LogLoadRunner{
		queue: queue,
		fn:    common.Mana.LoggerPolicy.Load(),
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
		dblog.GetAllPolicy().Range(func(key, value interface{}) bool {
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
			fn:    common.Mana.LoggerPolicy.Store(),
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
