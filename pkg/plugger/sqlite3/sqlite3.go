package sqlite3

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SqliteDriver struct {
	DB *gorm.DB
}

func NewSqliteDriver(dbName string) (*SqliteDriver, error) {
	db, err := gorm.Open(sqlite.Open(dbName))
	if err != nil {
		return nil, err
	}

	return &SqliteDriver{
		DB: db,
	}, nil
}

func (d SqliteDriver) Initalial() {}
