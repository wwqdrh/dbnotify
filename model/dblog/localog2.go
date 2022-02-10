package dblog

import (
	"fmt"
	"strings"
	"time"

	"github.com/wwqdrh/datamanager/common/datautil"
	"github.com/wwqdrh/datamanager/global"
)

// 另一种log key的设计方式
// 时间戳-主键值 = 记录
// 这样能减少冗余 删除过期日志记录更加方便 唯一需要额外处理的是根据字段搜索记录需要全盘搜索 或者可以添加一个字段在key上进行标识

type (
	LocalLog2 struct {
	}

	ILocalLog interface {
		Write(tableName string, data []map[string]interface{}, outdate, minLog int) error // 入库
	}
)

func pagination(records []map[string]interface{}, page, pageSize int) []map[string]interface{} {
	if page <= 0 {
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
	if maxItem > len(records) {
		maxItem = len(records)
	}
	return records[offset:maxItem]
}

func (l LocalLog2) GetKeyBuilder(date, field string) string {
	return fmt.Sprintf("%s@%s", date, field)
}

func (l LocalLog2) GetKeyPin(field string) string {
	return fmt.Sprintf("0000-00-00@%s", field)
}

func (l LocalLog2) GetLastKeyPin(field string) string {
	return fmt.Sprintf("9999-99-99@%s", field)
}

// 记录dml 写到磁盘中
func (l LocalLog2) Write(tableName string, data []map[string]interface{}, outdate, minLog int) error {
	if len(data) == 0 {
		return nil
	}

	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	_, err := global.G_LOGDB.GetDB(dbName)
	if err != nil {
		return err
	}

	primaryFields, err := global.G_DATADB.GetPrimary(tableName)
	if err != nil {
		return err
	}
	primaryStr := strings.Join(primaryFields, ",") + "="
	// 字段-时间戳 需要先加一个字段-0000的分界线方便删除
	for _, item := range data {
		switch item["action"] {
		case "ddl":
			l.WriteDDL(tableName, item, dbName)
		default:
			l.WriteDML(tableName, item, dbName, primaryFields, primaryStr, outdate, minLog)
		}

	}

	return nil
}

func (l LocalLog2) WriteDML(tableName string, item map[string]interface{}, dbName string, primaryFields []string, primaryStr string, outdate, minLog int) error {
	logs := item["log"].(map[string]interface{})
	datetime := item["time"].(string)[:10]

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
	return l.AddLog(dbName, datetime, primaryStr+strings.Join(primaryValStr, ","), item, outdate, minLog)

}

// 记录ddl 语句
func (l LocalLog2) WriteDDL(tableName string, item map[string]interface{}, dbName string) error {
	datetime := item["time"].(string)[:10]
	return l.AddLog(dbName, datetime, "type=ddl", item, -1, -1)
}

// 添加一条记录 过期日期需要删除
func (l LocalLog2) AddLog(db string, date, field string, record map[string]interface{}, outdate, minLog int) error {
	if err := l.addPin(db, date, field); err != nil {
		return err
	}

	// 设置为-1永远不删除，比如ddl相关的
	if outdate > 0 && minLog > 0 {
		l.removeRecord(db, date, field, outdate, minLog)
	}

	key := l.GetKeyBuilder(date, field)
	err := global.G_LOGDB.WriteByArray(db, key, record)
	if err != nil {
		return err
	}

	return nil
}

func (l LocalLog2) addPin(dbName, date, field string) error {
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
func (l LocalLog2) removeRecord(dbName, date, field string, day int, minNum int) error {
	datetime := time.Now().AddDate(0, 0, -day)
	start, end := l.GetKeyPin(field), l.GetKeyBuilder(datautil.ParseTime(&datetime)[:10], field)
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

func (l LocalLog2) GetTimeRange(key string, start, end *time.Time) (string, string) {
	var startstr, endstr string
	if start == nil && end == nil {
		startstr, endstr = l.GetKeyPin(key), l.GetLastKeyPin(key)
	} else if start == nil && end != nil {
		startstr, endstr = l.GetKeyPin(key), l.GetKeyBuilder(datautil.ParseTime(end)[:10], key)
	} else if start != nil && end == nil {
		startstr, endstr = l.GetKeyBuilder(datautil.ParseTime(start)[:10], key), l.GetLastKeyPin(key)
	} else {
		startstr, endstr = l.GetKeyBuilder(datautil.ParseTime(start)[:10], key), l.GetKeyBuilder(datautil.ParseTime(end)[:10], key)
	}
	return startstr, endstr
}

// 获取整体记录信息
func (l LocalLog2) SearchAllRecord(tableName string, primaryFields []string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	primaryStr := strings.Join(primaryFields, ",") + "="
	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	startstr, endstr := l.GetTimeRange(primaryStr, start, end)
	res, err := global.G_LOGDB.IteratorByRange(dbName, startstr, endstr)
	if err != nil {
		return nil, err
	}

	// 构造结果 不包括具体记录
	result := []map[string]interface{}{}
	for _, item := range res {
		if strings.Index(item["key"].(string), "0000-00-00") == 0 || strings.Index(item["key"].(string), "9999-99-99") == 0 {
			continue
		}
		cur := item["data"].([]map[string]interface{})
		cur[0] = map[string]interface{}{
			"action": cur[0]["action"],
			"time":   cur[0]["time"],
		}
		result = append(result, item)
	}

	return pagination(result, page, pageSize), nil
}

// SearchRecordByField 获取具体记录值
func (l LocalLog2) SearchRecordByField(tableName string, primaryID string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	// primaryStr := strings.Join(primaryFields, ",") + "="
	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	startstr, endstr := l.GetTimeRange(primaryID, start, end)
	res, err := global.G_LOGDB.IteratorByRange(dbName, startstr, endstr)
	if err != nil {
		return nil, err
	}

	// 将0000-00-00 9999-99-99 其他的key 的去除
	result := []map[string]interface{}{}
	for _, item := range res {
		if strings.Index(item["key"].(string), "0000-00-00") == 0 ||
			strings.Index(item["key"].(string), "9999-99-99") == 0 ||
			strings.Index(item["key"].(string), primaryID) == -1 {
			continue
		}
		result = append(result, item)
	}
	// 将result[...]["data"]中的进行组合
	// records := []map[string]interface{}{}
	// for _, item := range result {
	// 	records = append(records, item["data"].([]map[string]interface{})...)
	// }

	return pagination(result, page, pageSize), nil
}

// SearchRecordWithCondition 根据表名、字段条件进行查询
func (l LocalLog2) SearchRecordWithCondition(tableName string, primary []string, condition map[string]string, start, end *time.Time, page, pageSize int) ([]map[string]interface{}, error) {
	dbName := tableName + "_log.db" // 一般都是同一个数据表的数据
	startstr, endstr := l.GetTimeRange(strings.Join(primary, ",")+"=", start, end)
	res, err := global.G_LOGDB.IteratorByRange(dbName, startstr, endstr)
	if err != nil {
		return nil, err
	}

	// 将0000-00-00 9999-99-99 其他的key 的去除
	result := []map[string]interface{}{}
	for _, item := range res {
		if strings.Index(item["key"].(string), "0000-00-00") == 0 ||
			strings.Index(item["key"].(string), "9999-99-99") == 0 {
			continue
		}
		result = append(result, item)
	}
	// 将result[...]["data"]中的进行组合
	records := []map[string]interface{}{}
	for _, item := range result {
		for _, record := range item["data"].([]map[string]interface{}) {
			log := record["log"].(map[string]interface{})
			all := log["all"].(map[string]interface{})

			flag := true
			for key, val := range condition {
				if fmt.Sprint(all[key]) != val {
					flag = false
					break
				}
			}
			if !flag {
				continue
			}

			records = append(records, record)
		}
	}

	return pagination(records, page, pageSize), nil
}
