package driver

import (
	"datamanager/pkg/plugger/sqlite3"
	"sync"
)

// sqlite3的driver

var SqliteDriver *sqlite3.SqliteDriver
var SqliteOnce = sync.Once{}

// 初始化driver
func InitSqliteDriver() (e error) {
	SqliteOnce.Do(func() {
		driver, err := sqlite3.NewSqliteDriver("./version/version.db")
		if err != nil {
			e = err
		} else {
			driver.Initalial()
			SqliteDriver = driver
		}
	})
	return
}
