package main

import (
	"encoding/json"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
	// example1()
	example2()
}

func example1() {
	//定义字符数组key value
	key := []byte("hello")
	value := []byte("hi i'm levelDB by go")
	//定义字符串的键值对
	k1 := "hhh"
	v1 := "heiheiheihei"

	// The returned DB instance is safe for concurrent use.
	// The DB must be closed after use, by calling Close method.
	//文件夹位置,获取db实际
	db, err := leveldb.OpenFile("version.db", nil)

	//延迟关闭db流,必须的操作
	defer db.Close()
	if err != nil {
		fmt.Println(err)
	}
	//向db中插入键值对
	e := db.Put(key, value, nil)
	//将字符串转换为byte数组
	e = db.Put([]byte(k1), []byte(v1), nil)
	fmt.Println(e) //<nil>
	/**
	  || 注意:查看路径下的db文件,可知道文件都是自动切分为一些小文件.
	*/
	//根据key值查询value
	data, _ := db.Get(key, nil)
	fmt.Println(data)        //[104 105 32 105 39 109 32 108 101 118 101 108 68 66 32 98 121 32 103 111]
	fmt.Printf("%c\n", data) //[h i   i ' m   l e v e l D B   b y   g o]
	data, _ = db.Get([]byte(k1), nil)
	fmt.Println(data)        //[104 101 105 104 101 105 104 101 105 104 101 105]
	fmt.Printf("%c\n", data) //[h e i h e i h e i h e i]
	// It is safe to modify the contents of the arguments after Delete returns but
	// not before.
	//根据key进行删除操作
	i := db.Delete(key, nil)
	fmt.Println(i) //<nil>
	data, _ = db.Get(key, nil)
	fmt.Println(data) //[]
	//获取db快照
	snapshot, i := db.GetSnapshot()
	fmt.Println(snapshot) //leveldb.Snapshot{22}
	fmt.Println(i)        //<nil>
	//注意: The snapshot must be released after use, by calling Release method.
	//也就是说snapshot在使用之后,必须使用它的Release方法释放!
	snapshot.Release()
}

func example2() {
	fmt.Println("hello")

	// 创建或打开一个数据库
	db, err := leveldb.OpenFile("version.db", nil)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	// 存入数据
	db.Put([]byte("1"), []byte("6"), nil)
	db.Put([]byte("2"), []byte("7"), nil)
	db.Put([]byte("3"), []byte("8"), nil)
	db.Put([]byte("foo-4"), []byte("9"), nil)
	db.Put([]byte("5"), []byte("10"), nil)
	db.Put([]byte("6"), []byte("11"), nil)
	db.Put([]byte("moo-7"), []byte("12"), nil)
	db.Put([]byte("8"), []byte("13"), nil)

	// 遍历数据库内容
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		panic(err)

	}

	fmt.Println("***************************************************")

	// 删除某条数据
	err = db.Delete([]byte("2"), nil)

	// 读取某条数据
	data, err := db.Get([]byte("2"), nil)
	fmt.Printf("[2]:%s:%s\n", data, err)

	// 根据前缀遍历数据库内容
	fmt.Println("***************************************************")
	iter = db.NewIterator(util.BytesPrefix([]byte("foo-")), nil)
	for iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
	err = iter.Error()

	// 遍历从指定 key
	fmt.Println("***************************************************")
	iter = db.NewIterator(nil, nil)
	for ok := iter.Seek([]byte("5")); ok; ok = iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
	err = iter.Error()

	// 遍历子集范围
	fmt.Println("***************************************************")
	iter = db.NewIterator(&util.Range{Start: []byte("foo"), Limit: []byte("loo")}, nil)
	for iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
	err = iter.Error()

	// 批量操作
	fmt.Println("***************************************************")
	batch := new(leveldb.Batch)
	batch.Put([]byte("foo"), []byte("value"))
	batch.Put([]byte("bar"), []byte("another value"))
	batch.Delete([]byte("baz"))
	err = db.Write(batch, nil)

	// 遍历数据库内容
	iter = db.NewIterator(nil, nil)
	for iter.Next() {
		fmt.Printf("[%s]:%s\n", iter.Key(), iter.Value())
	}
	iter.Release()
	err = iter.Error()

	// 添加数组json格式
	// 存入数据
	databyte, err := json.Marshal([]map[string]interface{}{
		{"name": "1", "value": "2"},
		{"name": "2", "value": "3"},
	})
	db.Put([]byte("2020-12-30-name"), databyte, nil)
	data, err = db.Get([]byte("2020-12-30-name"), nil)
	fmt.Printf("%s: \n", data)
}
