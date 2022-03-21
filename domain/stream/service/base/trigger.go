package base

import (
	"context"

	"github.com/wwqdrh/datamanager/domain/stream/entity"
)

// stream的初始化接口
type IStreamInitial interface {
	Install(context.Context) (chan map[string]interface{}, error) // 执行初始化问题, 返回通道用于exporter来获取数据并使用
	Start(tableName string, id uint64, num int) error             // 开启读取日志并发送消息
}

// stream的策略接口
type IStreamPolicy interface {
	GetAllPolicy() map[string]*entity.Policy               // 获取所有的表格当前策略
	ModifyOutdate(tableName string, outdate int) error     // 修改表格的过期时间
	ModifyField(tableName string, fields []string) error   // 修改监听字段
	ModifyMinLogNum(tableName string, minLogNum int) error // 修改最小日志数
}
