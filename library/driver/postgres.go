package driver

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm/logger"
)

////////////////////
// types>>>
////////////////////

type JSON json.RawMessage

// 实现 sql.Scanner 接口，Scan 将 value 扫描至 Jsonb
func (j *JSON) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New(fmt.Sprint("Failed to unmarshal JSONB value:", value))
	}

	result := json.RawMessage{}
	err := json.Unmarshal(bytes, &result)
	*j = JSON(result)
	return err
}

// 实现 driver.Valuer 接口，Value 返回 json value
func (j JSON) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return json.RawMessage(j).MarshalJSON()
}

////////////////////
// <<<types
////////////////////

var defaultLogger = logger.New(
	log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
	logger.Config{
		SlowThreshold:             time.Second,   // Slow SQL threshold
		LogLevel:                  logger.Silent, // Log level
		IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
		Colorful:                  false,         // Disable color
	},
)

type PostgresDriver struct {
	account         string
	password        string
	host            string
	dbName          string
	charset         string // utf8
	ParseTime       bool   // true
	loc             string // Local
	maxIdleConns    int
	maxOpenConns    int
	connMaxLifetime time.Duration
	db              *gorm.DB
}

func InitPostgresDriver(username, password, path, dbName string) (*PostgresDriver, error) {
	driver := &PostgresDriver{
		account:         username,
		password:        password,
		host:            path,
		dbName:          dbName,
		charset:         "utf8",
		ParseTime:       true,
		loc:             "Asia/Shanghai",
		maxIdleConns:    10,
		maxOpenConns:    100,
		connMaxLifetime: time.Hour,
	}
	if err := driver.Connection(); err != nil {
		return nil, err
	}
	return driver, nil
}

func (d *PostgresDriver) DB() *gorm.DB {
	return d.db
}

func (d *PostgresDriver) DNS(db bool) string {
	if db {
		return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", d.account, d.password, d.host, d.dbName)
	} else {
		return fmt.Sprintf("postgres://%s:%s@%s?sslmode=disable", d.account, d.password, d.host)
	}
}

func (d *PostgresDriver) Connection() error {
	if d.db != nil {
		return nil
	}

	try := func() (*gorm.DB, error) {
		db, err := gorm.Open(postgres.New(postgres.Config{
			DSN: d.DNS(true), // data source name // auto configure based on currently Postgres version
		}), &gorm.Config{
			Logger: defaultLogger,
		})
		if err != nil {
			return nil, err
		}
		return db, nil
	}

	if db, err := try(); err != nil {
		if err := d.CreateDatabase(d.dbName); err != nil {
			return err
		}
		if db, err := try(); err != nil {
			return err
		} else {
			d.db = db
		}
	} else {
		d.db = db
	}

	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(d.maxIdleConns)

	// SetMaxOpenConns sets the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(d.maxOpenConns)

	// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
	sqlDB.SetConnMaxLifetime(d.connMaxLifetime)

	return nil
}

// CreateDatabase 创建新的数据库 指定名字，指定charset 引擎 之类
func (d *PostgresDriver) CreateDatabase(name string) error {
	db, err := sql.Open("postgres", d.DNS(false))
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER "postgres" ENCODING 'UTF8';`, name))
	if err != nil {
		return err
	}

	return nil
}

func (d *PostgresDriver) InitWithDB(db *gorm.DB) *PostgresDriver {
	d.db = db
	return d
}

func (d *PostgresDriver) GetTableName(table interface{}) string {
	if val, ok := table.(string); ok {
		return val
	}
	stmt := &gorm.Statement{DB: d.db}
	stmt.Parse(table)
	tableName := stmt.Schema.Table
	return tableName
}

// 创建触发器是否成功
func (d *PostgresDriver) CreateTrigger(sql string, triggerName string) error {
	triggers := d.ListTrigger()
	if _, ok := triggers[triggerName]; ok {
		// return errors.New("触发器已经存在")
		return nil
	}

	if err := d.db.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}

func (d *PostgresDriver) CreateEventTrigger(sql, triggerName string) error {
	triggers := d.ListEventTrigger()
	if _, ok := triggers[triggerName]; ok {
		// return errors.New("触发器已经存在")
		return nil
	}
	if err := d.db.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}

func (d *PostgresDriver) DeleteTrigger(triggerName string) error {
	triggers := d.ListTrigger()
	if _, ok := triggers[triggerName]; !ok {
		return errors.New("触发器不存在")
	}

	if err := d.db.Exec("delete from pg_trigger where tgname = ?", triggerName).Error; err != nil {
		return err
	}
	return nil
}

func (d *PostgresDriver) ListTrigger() map[string]bool {
	var res map[string]bool = map[string]bool{}
	var triggerName string

	rows, err := d.db.Raw("select tgname from pg_trigger").Rows()
	if err != nil {
		return map[string]bool{}
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&triggerName)
		res[triggerName] = true
	}

	return res
}

func (d *PostgresDriver) ListEventTrigger() map[string]bool {
	// select * from pg_event_trigger;
	var res map[string]bool = map[string]bool{}
	var triggerName string

	rows, err := d.db.Raw("select evtname from pg_event_trigger").Rows()
	if err != nil {
		return map[string]bool{}
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&triggerName)
		res[triggerName] = true
	}

	return res
}
