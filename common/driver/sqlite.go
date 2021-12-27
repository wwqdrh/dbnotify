package driver

import (
	"datamanager/pkg/plugger/sqlite3_orm"
	"sync"
)

// sqlite3的driver

var SqliteDriver *sqlite3_orm.SqliteDriver
var SqliteOnce = sync.Once{}

// 初始化driver
func InitSqliteDriver() (e error) {
	SqliteOnce.Do(func() {
		driver, err := sqlite3_orm.NewSqliteDriver("./version.db")
		if err != nil {
			e = err
		} else {
			driver.Initalial()
			SqliteDriver = driver
		}
	})
	return
}
