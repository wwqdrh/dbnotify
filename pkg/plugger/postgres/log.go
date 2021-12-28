package postgres

import (
	"log"
	"os"
	"time"

	"gorm.io/gorm/logger"
)

// 也可以在创建gorm的时候快速指定log级别
// GORM defined log levels: Silent, Error, Warn, Info
// db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{
// 	Logger: logger.Default.LogMode(logger.Silent),
//   })

var defaultLogger = logger.New(
	log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
	logger.Config{
		SlowThreshold:             time.Second,   // Slow SQL threshold
		LogLevel:                  logger.Silent, // Log level
		IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
		Colorful:                  false,         // Disable color
	},
)
