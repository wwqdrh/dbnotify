package driver

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	goleveldb "github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// 支持按照文件名返回连接
type LevelDBDriver struct {
	dbMap  *sync.Map //  map[string]*goleveldb.DB
	prefix string
	mu     sync.Mutex // 用于并发新建连接的
}

func NewLevelDBDriver(prefix string) (*LevelDBDriver, error) {
	if err := os.MkdirAll(prefix, os.ModePerm); err != nil {
		return nil, err
	}
	return &LevelDBDriver{
		dbMap:  &sync.Map{}, // map[string]*goleveldb.DB{},
		prefix: prefix,
		mu:     sync.Mutex{},
	}, nil
}

func (l *LevelDBDriver) GetDBName(dbName string) string {
	return path.Join(l.prefix, dbName)
}

func (l *LevelDBDriver) GetDB(dbName string) (*goleveldb.DB, error) {
	dbName = l.GetDBName(dbName)
	if val, ok := l.dbMap.Load(dbName); ok {
		return val.(*goleveldb.DB), nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if val, ok := l.dbMap.Load(dbName); ok {
		return val.(*goleveldb.DB), nil
	}

	db, err := goleveldb.OpenFile(dbName, nil)
	if err != nil {
		return nil, err
	}
	l.dbMap.Store(dbName, db)
	return db, nil
}

func (l *LevelDBDriver) Close() error {
	l.dbMap.Range(func(key, value interface{}) bool {
		value.(*goleveldb.DB).Close()
		return true
	})
	return nil
}

func (l *LevelDBDriver) Truncate(dbName string) error {
	name := l.GetDBName(dbName)
	db, err := l.GetDB(dbName)
	if err != nil {
		return err
	}
	l.dbMap.Delete(name)
	db.Close()
	return os.RemoveAll(name)
}

// 根据前缀获取key，value
func (l *LevelDBDriver) IteratorByPrefix(dbName string, prefix string) (map[string][]byte, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil, errors.New("未初始化数据库")
	}

	res := make(map[string][]byte, 0)

	iter := db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		res[string(iter.Key())] = iter.Value()
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return res, nil
}

func (l *LevelDBDriver) IteratorByRange(dbName string, start, end string) ([]map[string]interface{}, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil, errors.New("未初始化数据库")
	}

	values := []map[string]interface{}{}
	iter := db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(end)}, nil)
	for iter.Next() {
		curItem := []map[string]interface{}{}
		json.Unmarshal(iter.Value(), &curItem)
		values = append(values, map[string]interface{}{"key": string(iter.Key()), "data": curItem})
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return nil, err
	}
	return values, nil
}

// IteratorStrByRange 按照范围获取key-value
func (l *LevelDBDriver) IteratorStrByRange(dbName string, start, end string) ([]map[string]string, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil, errors.New("未初始化数据库")
	}

	values := []map[string]string{}
	iter := db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(end)}, nil)
	for iter.Next() {
		values = append(values, map[string]string{"key": string(iter.Key()), "value": string(iter.Value())})
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return nil, err
	}
	return values, nil
}

// 写入数据 找给定key判断能否找到 能找到的话按照数组的格式往后添加数据
func (l *LevelDBDriver) WriteByArray(dbName string, key string, value interface{}) error {
	db, err := l.GetDB(dbName)
	if err != nil {
		return errors.New("未初始化数据库")
	}

	var res []interface{}

	data, err := db.Get([]byte(key), nil)
	if err != nil {
		res = []interface{}{value}
	} else {
		err = json.Unmarshal(data, &res)
		if err != nil {
			return err
		}
		res = append(res, value)
	}

	jsonbyte, err := json.Marshal(res)
	if err != nil {
		return err
	}

	db.Put([]byte(key), jsonbyte, nil)

	return nil
}

func (l *LevelDBDriver) GetArrJson(dbName string, key string) ([]map[string]interface{}, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil, errors.New("未初始化数据库")
	}

	data, err := db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	var res []map[string]interface{}
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (l *LevelDBDriver) Get(dbName, key string) ([]byte, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil, errors.New("未初始化数据库")
	}

	data, err := db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (l *LevelDBDriver) Put(dbName, key string, value []byte) error {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil
	}

	if err := db.Put([]byte(key), value, nil); err != nil {
		return err
	}
	return nil
}

func (l *LevelDBDriver) Delete(dbName, key string) error {
	db, err := l.GetDB(dbName)
	if err != nil {
		return err
	}

	batch := new(goleveldb.Batch)
	batch.Delete([]byte(key))
	if err := db.Write(batch, nil); err != nil {
		return err
	}
	return nil
}

func (l *LevelDBDriver) GetJson(dbName, key string) (map[string]interface{}, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return nil, errors.New("未初始化数据库")
	}

	data, err := db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	var res map[string]interface{}
	err = json.Unmarshal(data, &res)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (l *LevelDBDriver) WriteJson(dbName string, key string, value interface{}) error {
	db, err := l.GetDB(dbName)
	if err != nil {
		return errors.New("未初始化数据库")
	}

	databyte, err := json.Marshal(value)
	if err != nil {
		return err
	}

	return db.Put([]byte(key), databyte, nil)
}

// Remove 删除给定范围的数据 nums 指定删除的记录条数
// TODO: 需要把占位key的逻辑移除出去作为通用的
func (l *LevelDBDriver) Remove(dbName string, start string, end string, nums int) error {
	db, err := l.GetDB(dbName)
	if err != nil {
		return errors.New("未初始化数据库")
	}

	keys := [][]byte{}
	times := 0
	iter := db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(end)}, nil)
	for iter.Next() {
		keys = append(keys, iter.Key())
		times++
		if times == nums+1 { // 有一个占位key
			break
		}
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		return err
	}

	batch := new(goleveldb.Batch)
	for _, key := range keys[1:] {
		batch.Delete([]byte(key))
	}
	if err := db.Write(batch, nil); err != nil {
		return err
	}
	return nil
}

func (l *LevelDBDriver) GetRangeNum(dbName string, start string, end string) (int, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return -1, errors.New("未初始化数据库")
	}

	res := 0
	iter := db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(end)}, nil)
	for iter.Next() {
		res++
	}
	return res, nil
}

func (l *LevelDBDriver) IterAll(dbName string) error {
	db, err := l.GetDB(dbName)
	if err != nil {
		return errors.New("未初始化数据库")
	}

	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Printf("%s = %s \n", string(iter.Key()), string(iter.Value()))
	}

	return nil
}

func (l *LevelDBDriver) IterAllLen(dbName string) (int, error) {
	db, err := l.GetDB(dbName)
	if err != nil {
		return -1, errors.New("未初始化数据库")
	}

	iter := db.NewIterator(nil, nil)
	cnt := 0
	for iter.Next() {
		cnt++
	}

	return cnt, nil
}
