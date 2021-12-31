package model

import (
	"datamanager/pkg/datautil"
	"datamanager/pkg/plugger/leveldb"
	"encoding/json"
	"fmt"
	"time"
)

////////////////////
// 一个表一个数据库文件
// key按照字段-日期的方式进行存储(会有冗余每一个字段会存放的记录)
// 要查找时按照字段 然后过期时间之前的直接删除
// 删除是惰性删除 存的时候把字段+日期之前的进行删除
////////////////////

type (
	LocalLog struct {
		db *leveldb.LevelDBDriver
	}
)

// fields 只记录这里面的变更
func (l LocalLog) Write(tableName string, data []map[string]interface{}, fields []string, outdate int) error {
	if len(data) == 0 {
		return nil
	}

	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	_, err := l.db.GetDB(dbName)
	if err != nil {
		return err
	}

	// // 字段-时间戳 需要先加一个字段-0000的分界线方便删除
	for _, item := range data {
		// field 不是action 而是监听到的字段
		// wash(item.Action)
		logs := item["log"].(map[string]interface{})
		datetime := datautil.ParseTime(item["time"].(*time.Time))[:10]
		for field := range logs {
			l.AddLog(dbName, datetime, field, item, outdate)
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
func (l LocalLog) AddLog(db string, date, field string, record map[string]interface{}, outdate int) error {
	if err := l.addPin(db, date, field); err != nil {
		return err
	}

	l.removeRecord(db, date, field, outdate)

	key := l.GetKeyBuilder(field, date)
	err := l.db.WriteByArray(db, key, record)
	if err != nil {
		return err
	}

	return nil
}

func (l LocalLog) addPin(dbName, date, field string) error {
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

// 删除过期日志
func (l LocalLog) removeRecord(dbName, date, field string, day int) error {
	datetime := time.Now().AddDate(0, 0, -day)
	start, end := l.GetKeyPin(field), l.GetKeyBuilder(field, datautil.ParseTime(&datetime)[:10])
	return l.db.Remove(dbName, start, end)
}

// SearchRecord 按照字段名 以及开始结束时间进行搜索
func (l LocalLog) SearchRecord(dbName string, fields []string, start, end *time.Time) (map[string]interface{}, error) {
	res := map[string]interface{}{}
	for _, field := range fields {
		var item []map[string]interface{}
		var citem []map[string]interface{}
		if start == nil && end == nil {
			startstr, endstr := l.GetKeyPin(field), l.GetLastKeyPin(field)
			data, err := l.db.IteratorByRange(dbName, startstr, endstr)
			if err != nil {
				return nil, err
			}
			for _, i := range data {
				err := json.Unmarshal(i, &citem)
				if err != nil {
					return nil, err
				}
				item = append(item, citem...)
			}
		} else if start == nil && end != nil {
			startstr, endstr := l.GetKeyPin(field), l.GetKeyBuilder(field, datautil.ParseTime(end)[:10])
			data, err := l.db.IteratorByRange(dbName, startstr, endstr)
			if err != nil {
				return nil, err
			}
			for _, i := range data {
				err := json.Unmarshal(i, &citem)
				if err != nil {
					return nil, err
				}
				item = append(item, citem...)
			}

		}
		res[field] = item
	}

	return res, nil
}
