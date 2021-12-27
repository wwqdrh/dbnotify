package main

import (
	"datamanager/common/driver"
	"datamanager/core/initial"
	"datamanager/core/op"
	"fmt"
)

type Company struct {
	ID      int `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	Name    string
	Age     int
	Address string
	Salary  int
}

func (Company) TableName() string {
	return "company"
}

func main() {
	errs := driver.InitDriver()
	if len(errs) > 0 {
		fmt.Println(errs)
		return
	}

	db := driver.PostgresDriver.DB
	sqlitedb := driver.SqliteDriver.DB

	// act := db.Model(&Company{}).Where("id = ?", 1).Update("name", "new_name")
	// if err = act.Error; err != nil {
	// 	fmt.Println(err)
	// }
	initial.MigrateInitial(sqlitedb) // 版本数据库的初始化
	op.LoadTable(sqlitedb)
	op.RegisterDBCallback(db) // callback的注册

	if err := op.RegsiterTable(sqlitedb, "company", "now", []string{"name", "age"}); err != nil {
		fmt.Println(err)
		return
	} // 注册新的表
	// 然后后面的create、delete、select这些操作都是能够自动处理的
	// 构造正向以及反向的sql字符串 然后加入到version数据表中

	// session mode
	// stmt := db.Session(&gorm.Session{DryRun: true}).Model(&Company{}).Where("id = ?", 1).Update("name", "new_name").Statement
	// fmt.Println(stmt.SQL.String()) //=> SELECT * FROM `users` WHERE `id` = $1 ORDER BY `id`
	// fmt.Println(stmt.Vars)         //=> []interface{}{1}
	// fmt.Println(stmt.Dest)
	// stmt = db.Session(&gorm.Session{DryRun: true}).Exec(`UPDATE "company" SET "name"="new_name" WHERE id = 1`).Statement

	// fmt.Println(stmt.SQL.String()) //=> SELECT * FROM `users` WHERE `id` = $1 ORDER BY `id`
	// fmt.Println(stmt.Vars)         //=> []interface{}{1}
	// fmt.Println(stmt.Dest)

	// op.RegisterDBCallback(db)

	// // 新增
	db.Create(&Company{
		Name:    "张三",
		Age:     19,
		Address: "home",
		Salary:  100,
	})
	// // 修改
	// db.Model(&Company{}).Where("name=?", "张三").Update("age", "21")
	// // 删除
	db.Where("name=?", "张三").Delete(&Company{})

}
