package driver

import (
	"datamanager/pkg/plugger/leveldb"
	"sync"
)

var LevelDBDriver *leveldb.LevelDBDriver
var leveldbOnce = sync.Once{}

func InitLevelDBDriver() (e error) {
	leveldbOnce.Do(func() {
		driver, err := leveldb.NewLevelDBDriver()
		if err != nil {
			e = err
		} else {
			e = driver.Initalial()
			LevelDBDriver = driver
		}
	})
	return
}
