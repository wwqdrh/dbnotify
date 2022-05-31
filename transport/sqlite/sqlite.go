package sqlite

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

type ILogData interface {
	GetSchema() string
	GetTable() string
	GetType() string                   // 获取日志记录类型 ddl dml
	GetLabel() string                  // 具体标签 insert update delete | alter column, table
	GetTime() time.Time                // 获取日志记录时间
	GetPaylod() map[string]interface{} // 获取具体的负载对象
	GetChange() map[string]interface{} // 获取具体的负载对象
}

var (
	escape = regexp.MustCompile(`"`)
	// create table `schema.table``
	tableCreate = `
	CREATE TABLE IF NOT EXISTS %s (
		id     INTEGER PRIMARY KEY,
		op           INTEGER,
		recordID           INTEGER,
		payload TEXT,
		changes TEXT
	);
	`

	// insert record change
	recordInsert = `
	INSERT INTO %s (op, recordID, payload, changes) values (%d, %d, "%s", "%s")
	`

	// record search
	recordSearch = `
	select op, recordID, payload, changes from %s where payload like "%%%s%%"
	`
)

type SqliteTransport struct {
	driver *SqliteDriver
}

func NewSqliteTransport(dbName string) (*SqliteTransport, error) {
	driver, err := NewDriver(dbName)
	if err != nil {
		return nil, err
	}
	return &SqliteTransport{
		driver: driver,
	}, nil
}

func (p *SqliteTransport) Save(log ILogData) error {
	tableName := fmt.Sprintf("%s_%s", log.GetSchema(), log.GetTable())
	_, err := p.driver.db.Exec(fmt.Sprintf(tableCreate, tableName))
	if err != nil {
		return err
	}

	// save
	payload, _ := json.Marshal(log.GetPaylod())
	changes, _ := json.Marshal(log.GetChange())
	_, err = p.driver.db.Exec(
		fmt.Sprintf(recordInsert, tableName, 0, 0, escape.ReplaceAllString(string(payload), `""`), escape.ReplaceAllString(string(changes), `""`)),
	)
	return err
}

func (p *SqliteTransport) Load(string) ([]string, error) {
	return nil, errors.New("plain transport no implete this")
}

// search record by keyword(field=value)
func (p *SqliteTransport) Search(tableName, key string, value interface{}) ([]string, error) {
	keyword := fmt.Sprintf(`"%s":"%s"`, key, value)
	fmt.Printf(recordSearch, tableName, escape.ReplaceAllString(keyword, `""`))
	rows, err := p.driver.db.Query(
		fmt.Sprintf(recordSearch, tableName, escape.ReplaceAllString(keyword, `""`)),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := []string{} // payloads: ..., changes: ...
	for rows.Next() {
		var (
			op, recordID     int
			payload, changes string
		)
		err = rows.Scan(&op, &recordID, &payload, &changes)
		if err != nil {
			continue
		}
		res = append(res, fmt.Sprintf("payloads:%s;changes:%s", payload, changes))
	}

	return res, nil
}
