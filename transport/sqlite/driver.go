package sqlite

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteDriver struct {
	db *sql.DB
}

func NewDriver(dbName string) (*SqliteDriver, error) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		return nil, err
	}

	return &SqliteDriver{
		db: db,
	}, nil
}
