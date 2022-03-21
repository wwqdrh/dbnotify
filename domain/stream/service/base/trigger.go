package base

import (
	"context"

	"github.com/wwqdrh/datamanager/domain/stream/entity"
	"github.com/wwqdrh/datamanager/domain/stream/vo"
)

type ITriggerStream interface {
	IStreamInitial
	IStreamPolicy
}

// stream的初始化接口
type IStreamInitial interface {
	Install(...vo.TablePolicy) (chan map[string]interface{}, error)        // 执行初始化问题, 返回通道用于exporter来获取数据并使用
	Start(ctx context.Context, tableName string, id uint64, num int) error // 开启读取日志并发送消息
}

// stream的策略接口
type IStreamPolicy interface {
	GetAllPolicy() map[string]*entity.Policy               // 获取所有的表格当前策略
	AddPolicy(vo.TablePolicy) error                        // 添加新的表策略
	ModifyPolicy(...interface{}) error                     // 修改配置
	RemovePolicy(string) error                             // 删除对应表名的策略
	ListTableField(tableName string) []*vo.Fields          // 返回表的字段列表
	ModifyOutdate(tableName string, outdate int) error     // 修改表格的过期时间
	ModifyField(tableName string, fields []string) error   // 修改监听字段
	ModifyMinLogNum(tableName string, minLogNum int) error // 修改最小日志数
}
