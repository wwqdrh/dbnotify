package dblog

import (
	"fmt"
	"time"

	"github.com/wwqdrh/datamanager/common/datautil"
	"github.com/wwqdrh/datamanager/global"
)

////////////////////
// 一个表一个数据库文件
// key按照字段-日期的方式进行存储(会有冗余每一个字段会存放的记录)
// 要查找时按照字段 然后过期时间之前的直接删除
// 删除是惰性删除 存的时候把字段+日期之前的进行删除
////////////////////

type (
	LocalLog struct {
	}
)

// fields 只记录这里面的变更
func (l LocalLog) Write(tableName string, data []map[string]interface{}, fields []string, outdate, minLog int) error {
	if len(data) == 0 {
		return nil
	}

	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	_, err := global.G_LOGDB.GetDB(dbName)
	if err != nil {
		return err
	}

	// // 字段-时间戳 需要先加一个字段-0000的分界线方便删除
	for _, item := range data {
		logs := item["log"].(map[string]interface{})
		datetime := datautil.ParseTime(item["time"].(*time.Time))[:10]
		for field := range logs["data"].(map[string]interface{}) {
			l.AddLog(dbName, datetime, field, item, outdate, minLog)
		}
	}

	return nil
}

func (l LocalLog) GetKeyBuilder(field, date string) string {
	return fmt.Sprintf("%s@%s", field, date)
}

func (l LocalLog) GetKeyPin(field string) string {
	return fmt.Sprintf("%s@0000-00-00", field)
}

func (l LocalLog) GetLastKeyPin(field string) string {
	return fmt.Sprintf("%s@9999-99-99", field)
}

// 添加一条记录 过期日期需要删除
func (l LocalLog) AddLog(db string, date, field string, record map[string]interface{}, outdate, minLog int) error {
	if err := l.addPin(db, date, field); err != nil {
		return err
	}

	l.removeRecord(db, date, field, outdate, minLog)

	key := l.GetKeyBuilder(field, date)
	err := global.G_LOGDB.WriteByArray(db, key, record)
	if err != nil {
		return err
	}

	return nil
}

func (l LocalLog) addPin(dbName, date, field string) error {
	key := l.GetKeyPin(field)
	lastKey := l.GetLastKeyPin(field)
	_, err := global.G_LOGDB.Get(dbName, key)
	if err != nil {
		if err2 := global.G_LOGDB.Put(dbName, key, []byte("")); err2 != nil {
			return err2
		}
	}
	_, err = global.G_LOGDB.Get(dbName, lastKey)
	if err != nil {
		if err2 := global.G_LOGDB.Put(dbName, lastKey, []byte("")); err2 != nil {
			return err2
		}
	}

	return nil
}

// removeRecord 删除过期日志 当小于最小日志数时则不删除
func (l LocalLog) removeRecord(dbName, date, field string, day int, minNum int) error {
	datetime := time.Now().AddDate(0, 0, -day)
	start, end := l.GetKeyPin(field), l.GetKeyBuilder(field, datautil.ParseTime(&datetime)[:10])
	nums, err := global.G_LOGDB.GetRangeNum(dbName, start, end)
	nums = nums - 2 // 需要删除两个占位key
	if err != nil {
		return err
	}
	if nums <= minNum {
		return nil
	}
	return global.G_LOGDB.Remove(dbName, start, end, nums-minNum)
}

// SearchRecord 按照字段名 以及开始结束时间进行搜索
func (l LocalLog) SearchRecordByField(dbName string, fields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	res := []map[string]interface{}{}
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
	if maxItem > len(res) {
		maxItem = len(res)
	}

	return res[offset:maxItem], nil
}
