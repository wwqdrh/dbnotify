package triggerstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wwqdrh/datamanager/domain/stream/entity"
	"github.com/wwqdrh/datamanager/domain/stream/repository"
	"github.com/wwqdrh/datamanager/domain/stream/service/base"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
	"github.com/wwqdrh/datamanager/runtime"
)

// 通过触发器全量记录所有的操作

var R = runtime.Runtime

type triggerStream struct {
	policys sync.Map
	ch      chan<- map[string]interface{}
}

func NewTriggerStream() base.ITriggerStream {
	return &triggerStream{
		policys: sync.Map{},
	}
}

// 为表格设置触发器、创建策略表
func (s *triggerStream) Install(tables ...vo.TablePolicy) (chan map[string]interface{}, error) {
	// 初始化
	// 表策略
	repository.PolicyRepo.Migrate(R.GetDB().DB)
	// 读取策略
	for _, item := range repository.PolicyRepo.GetAllData(R.GetDB().DB) {
		s.policys.Store(item.TableName, item)
	}
	// 添加日志表
	R.GetDB().DB.Table(R.GetConfig().TempLogTable).AutoMigrate(&entity.LogTable{})

	for _, table := range tables {
		// s.Register(table, s.MinLogNum, s.OutDate, nil, nil)
		if err := s.registerWithPolicy(table); err != nil {
			fmt.Println(err)
		}
	}

	// 结构体
	ch := make(chan map[string]interface{}, 10)
	s.ch = ch

	return ch, nil
} // 执行初始化问题, 返回通道用于exporter来获取数据并使用

// 根据表策略进行注册
func (s *triggerStream) registerWithPolicy(pol vo.TablePolicy) error {
	table := pol.Table
	if !s.registerCheck(table) { // 检查table是否合法
		return fmt.Errorf("%v不合法", table)
	}

	tableName := R.GetDB().GetTableName(table)
	if tableName == "" {
		return errors.New("表名不能为空")
	}

	if _, ok := s.policys.Load(tableName); !ok {
		if _, err := buildPolicy(tableName, 10, 10, pol); err != nil {
			return err
		} else {
			// DblogService.SetSenseFields(tableName, strings.Split(p.Fields, ","))
		}

		if err := buildTrigger(tableName, R.GetConfig().TempLogTable); err != nil {
			return err
		}
	}
	return nil
}

// 检查是否合法
func (s *triggerStream) registerCheck(table interface{}) bool {
	if val, ok := table.(string); ok {
		// 判断表是否存在
		tables := R.GetFieldHandler().GetTables()
		for _, item := range tables {
			if item.TableID == val {
				return true
			}
		}
		return false
	} else {
		R.GetDB().DB.AutoMigrate(table)
		return true
	}
}

func (s *triggerStream) Start(ctx context.Context, tableName string, id uint64, num int) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("退出")
		default:
			data, err := repository.LogTableRepo.ReadBytableNameAndLimit(tableName, id, num)
			if err == nil {
				d := map[string]interface{}{
					"datar": data,
				}
				s.ch <- d
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

// 每隔一段时间读取n条数据加入到任务队列中
func (s *triggerStream) start() {
	var id uint64

	for {
		for tableName, _ := range s.GetAllPolicy() {
			err := s.Start(context.TODO(), tableName, id, R.GetConfig().PerReadNum)
			if err != nil {
				fmt.Println("获取数据失败", err.Error())
			}
			// 删除执行完成的节点
		}
		time.Sleep(time.Minute * 10)
	}
}

func (s *triggerStream) GetAllPolicy() map[string]*entity.Policy {
	return nil
} // 获取所有的表格当前策略

func (s *triggerStream) AddPolicy(vo.TablePolicy) error {
	return nil
}

func (s *triggerStream) ModifyPolicy(...interface{}) error {
	return nil
}

func (s *triggerStream) RemovePolicy(tableName string) error {
	return nil
}

func (s *triggerStream) ModifyOutdate(tableName string, outdate int) error {
	return nil
} // 修改表格的过期时间

func (s *triggerStream) ModifyField(tableName string, fields []string) error {
	return nil
} // 修改监听字段

func (s *triggerStream) ModifyMinLogNum(tableName string, minLogNum int) error {
	return nil
}

func (s *triggerStream) ListTableField(tableName string) []*vo.Fields {
	return nil
}
