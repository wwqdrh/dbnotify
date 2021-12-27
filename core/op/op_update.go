package op

import "fmt"

// 更新语句
type UpdateOperator struct {
	*BaseOperator
	// rawSql string // 原始数据 update [table] set [key]=[value] where [key]=[value]
	oldValue map[string]string // 需要存储原始的数据
}

// NewUpdateOperator update操作的构建
// 需要知道操作的表是谁 更新的字段与值的映射 条件是什么
func NewUpdateOperator(sql string) (IOperator, error) {
	base, err := NewBaseOperator(sql)
	if err != nil {
		return nil, err
	}

	return &UpdateOperator{
		BaseOperator: base,
		oldValue:     map[string]string{},
	}, nil
}

// 初始化
func (o *UpdateOperator) Initatial() error {
	// TODO 这里模拟历史数据
	for field := range o.dest {
		o.oldValue[field] = `"mock_old_value"`
	}
	return nil
}

// 正向执行
func (o *UpdateOperator) ExecSQL() string {
	return o.rawSql
}

// 反向执行 update的反向执行sql语句
// 需要有update字段的原始的记录
// update [table] set key=value where key=value
func (o *UpdateOperator) ReverSQL() string {
	destStr := ""
	condStr := ""
	for key, value := range o.oldValue {
		destStr += fmt.Sprintf("%s=%s", key, value)
	}
	for key, value := range o.condition {
		condStr += fmt.Sprintf("%s=%s", key, value)
	}
	return fmt.Sprintf(`UPDATE %s SET %s WHERE %s`, o.tableName, destStr, condStr)
}
