package model

import (
	"datamanager/pkg/datautil"
	"datamanager/pkg/plugger"
	"fmt"
	"strings"
	"time"
)

// 另一种log key的设计方式
// 时间戳-主键值 = 记录
// 这样能减少冗余 删除过期日志记录更加方便 唯一需要额外处理的是根据字段搜索记录需要全盘搜索 或者可以添加一个字段在key上进行标识

type (
	LocalLog2 struct {
		db       *plugger.LevelDBDriver
		targetDB *plugger.PostgresDriver
	}

	ILocalLog interface {
		Write(tableName string, data []map[string]interface{}, outdate, minLog int) error // 入库
	}
)

func (l LocalLog2) GetKeyBuilder(date, field string) string {
	return fmt.Sprintf("%s@%s", date, field)
}

func (l LocalLog2) GetKeyPin(field string) string {
	return fmt.Sprintf("0000-00-00@%s", field)
}

func (l LocalLog2) GetLastKeyPin(field string) string {
	return fmt.Sprintf("9999-99-99@%s", field)
}

// Write 写到磁盘中
func (l LocalLog2) Write(tableName string, data []map[string]interface{}, outdate, minLog int) error {
	if len(data) == 0 {
		return nil
	}

	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	_, err := l.db.GetDB(dbName)
	if err != nil {
		return err
	}

	primaryFields, err := l.targetDB.GetPrimary(tableName)
	if err != nil {
		return err
	}
	primaryStr := strings.Join(primaryFields, ",") + "="
	// 字段-时间戳 需要先加一个字段-0000的分界线方便删除
	for _, item := range data {
		logs := item["log"].(map[string]interface{})
		datetime := datautil.ParseTime(item["time"].(*time.Time))[:10]

		// data := logs["data"].(map[string]interface{})
		logPrimaryData := logs["primary"].(map[string]interface{})
		primaryValStr := []string{} // 存储主键的值作为key
		for _, key := range primaryFields {
			primaryValue := logPrimaryData[key]
			if curValue, ok := primaryValue.(map[string]interface{}); ok {
				primaryValStr = append(primaryValStr, fmt.Sprintf("%v", curValue["before"]))
			} else {
				primaryValStr = append(primaryValStr, fmt.Sprintf("%v", logPrimaryData[key]))
			}
		}
		l.AddLog(dbName, datetime, primaryStr+strings.Join(primaryValStr, ","), item, outdate, minLog)
	}

	return nil
}

// 添加一条记录 过期日期需要删除
func (l LocalLog2) AddLog(db string, date, field string, record map[string]interface{}, outdate, minLog int) error {
	if err := l.addPin(db, date, field); err != nil {
		return err
	}

	l.removeRecord(db, date, field, outdate, minLog)

	key := l.GetKeyBuilder(date, field)
	err := l.db.WriteByArray(db, key, record)
	if err != nil {
		return err
	}

	return nil
}

func (l LocalLog2) addPin(dbName, date, field string) error {
	key := l.GetKeyPin(field)
	lastKey := l.GetLastKeyPin(field)
	_, err := l.db.Get(dbName, key)
	if err != nil {
		if err2 := l.db.Put(dbName, key, []byte("")); err2 != nil {
			return err2
		}
	}
	_, err = l.db.Get(dbName, lastKey)
	if err != nil {
		if err2 := l.db.Put(dbName, lastKey, []byte("")); err2 != nil {
			return err2
		}
	}

	return nil
}

// removeRecord 删除过期日志 当小于最小日志数时则不删除
func (l LocalLog2) removeRecord(dbName, date, field string, day int, minNum int) error {
	datetime := time.Now().AddDate(0, 0, -day)
	start, end := l.GetKeyPin(field), l.GetKeyBuilder(datautil.ParseTime(&datetime)[:10], field)
	nums, err := l.db.GetRangeNum(dbName, start, end)
	nums = nums - 2 // 需要删除两个占位key
	if err != nil {
		return err
	}
	if nums <= minNum {
		return nil
	}
	return l.db.Remove(dbName, start, end, nums-minNum)
}

// SearchRecord 按照字段名 以及开始结束时间进行搜索
func (l LocalLog2) SearchRecordByField(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	primaryStr := strings.Join(primaryFields, ",") + "="

	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据

	var startstr, endstr string
	if start == nil && end == nil {
		startstr, endstr = l.GetKeyPin(primaryStr), l.GetLastKeyPin(primaryStr)
	} else if start == nil && end != nil {
		startstr, endstr = l.GetKeyPin(primaryStr), l.GetKeyBuilder(primaryStr, datautil.ParseTime(end)[:10])
	} else if start != nil && end != nil {
		startstr, endstr = l.GetKeyBuilder(primaryStr, datautil.ParseTime(start)[:10]), l.GetLastKeyPin(primaryStr)
	} else {
		startstr, endstr = l.GetKeyBuilder(primaryStr, datautil.ParseTime(start)[:10]), l.GetKeyBuilder(primaryStr, datautil.ParseTime(end)[:10])
	}
	res, err := l.db.IteratorByRange(dbName, startstr, endstr)
	if err != nil {
		return nil, err
	}

	// 将0000-00-00 9999-99-99 的去除
	result := []map[string]interface{}{}
	for _, item := range res {
		if strings.Index(item["key"].(string), "0000-00-00") == 0 || strings.Index(item["key"].(string), "9999-99-99") == 0 {
			continue
		}
		result = append(result, item)
	}

	if page == 0 {
		page = 1
	}
	switch {
	case pageSize > 100:
		pageSize = 100
	case pageSize <= 0:
		pageSize = 10
	}
	offset := (page - 1) * pageSize
	maxItem := offset + pageSize
	if maxItem > len(result) {
		maxItem = len(result)
	}

	return result[offset:maxItem], nil
}
