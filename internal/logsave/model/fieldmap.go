package model

import (
	"fmt"
	"strings"

	"github.com/wwqdrh/datamanager/internal/driver"
)

////////////////////
// 存储表field的id与name的映射关系 只做修改和添加 不做删除
////////////////////

type (
	FieldMapping struct {
		FieldID   string
		FieldName string
	}
)

// GetRangeKey 获取边界状态
func (m FieldMapping) GetRangeKey() []string {
	return []string{"@key-00000000", "@key-zzzzzzzz"} // 大部分id都是数字和字母 这个可以作为边界
}

// GetKey key构造
func (FieldMapping) GetKey(fieldID string, flag bool) string {
	if flag {
		return "@key-" + fieldID
	} else {
		return strings.TrimLeft(fieldID, "@key-")
	}

}

// GetValue value构造
func (m FieldMapping) GetValue(fieldName string, flag bool) string {
	return m.FieldName
}

// ListAll 获取leveldb中的映射关系
func (m FieldMapping) ListAll(db *driver.LevelDBDriver, tableName string) ([]*FieldMapping, error) {
	var start, end string
	{
		t := m.GetRangeKey()
		start, end = t[0], t[1]
	}
	data, err := db.IteratorStrByRange(tableName, start, end)
	if err != nil {
		return nil, err
	}

	result := []*FieldMapping{}
	for _, item := range data {
		result = append(result, &FieldMapping{
			FieldID:   m.GetKey(item["key"], false),
			FieldName: item["value"],
		})
	}
	return result, nil
}

func (m FieldMapping) Write(db *driver.LevelDBDriver, tableName string) error {
	return db.Put(tableName, m.GetKey(m.FieldID, true), []byte(m.GetValue(m.FieldName, true)))
}

func (m FieldMapping) ListAllFieldMapping(logdb *driver.LevelDBDriver, tableName string) map[string]string {
	val, err := m.ListAll(logdb, tableName)
	res := map[string]string{}
	if err != nil {
		return res
	}
	for _, item := range val {
		res[item.FieldID] = item.FieldName
	}
	return res
}

func (m FieldMapping) UpdateAllFieldByMapping(logdb *driver.LevelDBDriver, tableName string, records map[string]string) error {
	for key, val := range records {
		cur := &FieldMapping{FieldID: key, FieldName: val}
		if err := cur.Write(logdb, tableName); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}
