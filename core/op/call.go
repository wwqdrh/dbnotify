package op

import (
	"datamanager/core/initial"
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
)

var (
	tablePolicy     = map[string][]string{} // 表名=>字段数组
	tablePolicyOnce = sync.Once{}
)

// RegsiterTable 用来注册数据表 哪些表的操作是需要进行策略的
// 第一次肯定先初始化
func RegsiterTable(db *gorm.DB, tableName string, cacheDate string, fields []string) error {
	if err := initial.MigrateNewTablePolicy(db, tableName, fields); err != nil {
		return err
	}
	// 需要将记录添加内存中 下次使用则可以直接激活
	tablePolicy[tableName] = fields
	return nil
}

// LoadTable 系统启动后需要将整个的缓存策略进行读取在内存中，这样才能方便每次查询以及启用
func LoadTable(db *gorm.DB) {
	tablePolicyOnce.Do(func() {
		data := initial.FindAllTablePolicy(db)
		for _, item := range data {
			tablePolicy[item.TableName] = strings.Split(item.Fields, ",")
		}
	})
}

// CheckIsActive 检查是否激活 根据表的名字，表的字段，相关的操作来判断是否需要激活
func CheckIsActive(tableName string, fields []interface{}) bool {
	targets, ok := tablePolicy[tableName]
	if !ok {
		return false
	}

	visit := map[string]bool{}
	for _, item := range targets {
		visit[item] = true
	}

	for _, item := range fields {
		if _, ok := visit[item.(string)]; ok {
			return true
		}
	}
	return false
}

// 注册db的callback操作
func RegisterDBCallback(db *gorm.DB) {
	// before gorm:create
	db.Callback().Create().Before("gorm:create").Register("before_create", beforeCreate)

	// after gorm:create
	db.Callback().Create().After("gorm:create").Register("after_create", afterCreate)

	// after gorm:query
	// db.Callback().Query().After("gorm:query").Register("my_plugin:after_query", afterQuery)
	db.Callback().Delete().Before("gorm:delete").Register("before_delete", beforeDelete)
	// after gorm:delete
	db.Callback().Delete().After("gorm:delete").Register("after_delete", afterDelete)

	// before gorm:update
	db.Callback().Update().Before("gorm:update").Register("before_update", beforeUpdate)
	db.Callback().Update().After("gorm:update").Register("after_update", afterUpdate)
	// before gorm:create and after gorm:before_create
	// db.Callback().Create().Before("gorm:create").After("gorm:before_create").Register("my_plugin:before_create", beforeCreate)

	// before any other callbacks
	// db.Callback().Create().Before("*").Register("update_created_at", updateCreated)

	// after any other callbacks
	// db.Callback().Create().After("*").Register("update_created_at", updateCreated)
}

// beforeCreate 创建之前
func beforeCreate(db *gorm.DB) {
	if CheckIsActive(db.Statement.Table, []interface{}{"name", "age"}) {
		fmt.Println("---do something")
		fmt.Println("create转换成delete")
		fmt.Println(db.Statement.Table)          // 获取表名
		fmt.Println(db.Statement.Schema.DBNames) // 获取字段名
	}
	fmt.Println("beforecreate")
	fmt.Println(db.Statement.Dest)
	fmt.Println(db.Statement.Vars...)
	fmt.Println("-----")
}

// afterCreate 创建之后
func afterCreate(db *gorm.DB) {
	if CheckIsActive(db.Statement.Table, []interface{}{"name", "age"}) {
		fmt.Println("---do something")
		fmt.Println("判断是否执行成功，成功了话就将这个数据的版本进行落地")
		// 其实只需要查询插入数据后所对应的主键，然后直接delete就好
	}
	fmt.Println(db.Statement.Dest)
	fmt.Println(db.Statement.Vars...)
	fmt.Println("aftercreate")
}

// beforeDelete 删除之前
func beforeDelete(db *gorm.DB) {
	if CheckIsActive(db.Statement.Table, []interface{}{"name", "age"}) {
		fmt.Println("---do something")
		fmt.Println("delete转换成create")

	}
	fmt.Println(db.Statement.Dest)
	fmt.Println(db.Statement.Vars...)
	fmt.Println("beforedelete")
}

// afterDelete 删除之后
func afterDelete(db *gorm.DB) {
	if CheckIsActive(db.Statement.Table, []interface{}{"name", "age"}) {
		fmt.Println("---do something")
		fmt.Println("判断是否执行成功，成功了话就将这个数据的版本进行落地")
		fmt.Println("落地成功")
	}
	fmt.Println(db.Statement.Dest)
	fmt.Println(db.Statement.Vars...)
	fmt.Println("afterdelete")
}

// beforeUpdate 修改之前
func beforeUpdate(db *gorm.DB) {
	if CheckIsActive(db.Statement.Table, []interface{}{"name", "age"}) {
		fmt.Println("---do something")
		fmt.Println("根据目标修改值先去读取原始数据并存储起来")
	}
	fmt.Println(db.Statement.Dest)
	fmt.Println(db.Statement.Vars...)
	fmt.Println("beforeupdate")
}

// 修改之后
func afterUpdate(db *gorm.DB) {
	if CheckIsActive(db.Statement.Table, []interface{}{"name", "age"}) {
		fmt.Println("---do something")
		fmt.Println("判断是否执行成功，成功了话就将这个数据的版本进行落地")
	}
	fmt.Println(db.Statement.Dest)
	fmt.Println(db.Statement.Vars...)
	fmt.Println("afterupdate")
}
