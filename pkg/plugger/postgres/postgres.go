package postgres

import (
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// 创建gorm.DB对象的时候连接并没有被创建，在具体使用的时候才会创建。
// gorm内部，准确的说是database/sql内部会维护一个连接池，
// 可以通过参数设置最大空闲连接数，连接最大空闲时间等。使用者不需要管连接的创建和关闭。
// 配置、连接、数据库初始化  剩下的就是session、transaction等

// PostgresDriver Postgres的连接管理器
type PostgresDriver struct {
	config  *PostgresConfig
	DB      *gorm.DB
	Trigger *TriggerCore
}

func NewPostgresDriver(account string, password string, host string, db string) (*PostgresDriver, error) {
	driver, err := NewPostgresDriverWithConfig(NewPostgresConfig(account, password, host, db))
	if err != nil {
		return nil, err
	}
	return driver, nil
}

// NewPostgresDriver Postgresdriver的构造函数
// 需要做连通性测试
func NewPostgresDriverWithConfig(config *PostgresConfig) (*PostgresDriver, error) {
	driver := &PostgresDriver{
		config: config,
	}
	if err := driver.Ping(); err != nil {
		return nil, err
	}
	return driver, nil
}

//
func (d *PostgresDriver) GetTableName(table interface{}) string {
	stmt := &gorm.Statement{DB: d.DB}
	stmt.Parse(table)
	tableName := stmt.Schema.Table
	return tableName
}

// Ping 连通性测试 可以不指定数据库
func (d *PostgresDriver) Ping() error {
	if _, err := gorm.Open(postgres.Open(d.config.DryDNS()), &gorm.Config{
		Logger: defaultLogger,
	}); err != nil {
		return err
	}
	return nil
}

// CreateDatabase 创建新的数据库 指定名字，指定charset 引擎 之类
func (d *PostgresDriver) CreateDatabase(name string) error {
	db, err := sql.Open("Postgres", d.config.DryDNS())
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE DATABASE " + name + " default character set utf8mb4 collate utf8mb4_general_ci;")
	if err != nil {
		return err
	}

	return nil
}

// DeleteDatabase 删除数据库
func (d *PostgresDriver) DeleteDatabase(name string) error {
	db, err := sql.Open("Postgres", d.config.DryDNS())
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("DROP DATABASE " + name)
	if err != nil {
		return errors.New("数据库删除失败")
	}

	return nil
}

// Initalial 连通数据库 是需要指定数据库名字的情况下才能执行
func (d *PostgresDriver) Initalial(name string) error {
	if d.config.DBName == "" && name == "" {
		return errors.New("未指定数据库名字，不能初始化")
	}

	if name != "" {
		d.config.DBName = name
	}
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: d.config.DNS(), // data source name // auto configure based on currently Postgres version
	}), &gorm.Config{
		Logger: defaultLogger,
	})
	if err != nil {
		return err
	}

	// connection pool 设置
	// Get generic database object sql.DB to use its functions
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(d.config.MaxIdleConns)

	// SetMaxOpenConns sets the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(d.config.MaxOpenConns)

	// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
	sqlDB.SetConnMaxLifetime(d.config.ConnMaxLifetime)

	d.DB = db
	d.Trigger = NewTriggerCore(db)
	return nil
}

// 从连接池中获取连接 Postgres中的由gorm内部来管理
func (d *PostgresDriver) Connection() interface{} {
	return d.DB
}

// Migrator 用于migrate数据表
func (d *PostgresDriver) Migrator(value ...interface{}) error {
	var err error
	for _, item := range value {
		if err = d.DB.AutoMigrate(item); err != nil {
			return err
		}
	}
	return nil
}

// ExecSQL 直接执行sql语句
func (d *PostgresDriver) ExecSQL(sql string, values ...interface{}) error {
	d.DB.Exec(sql, values...)
	return nil
}

// CreateTrigger 创建触发器
// 提供表名 操作类型 额外操作 自动生成所需要的sql创建触发器
func (d *PostgresDriver) CreateTrigger() error {
	return nil
}

func (d *PostgresDriver) AddColumn(tableName string, columnName string, columnType string) error {
	sql := fmt.Sprintf(`
DO
$do$
BEGIN
IF (SELECT COUNT(*) AS ct1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s' ) = 0
THEN
   ALTER TABLE %s ADD COLUMN %s %s not null;
END IF;
END;
$do$`, tableName, columnName, tableName, columnName, columnType)
	return d.DB.Exec(sql).Error
}

func (d *PostgresDriver) GetPrimary(table interface{}) []string {
	stmt := &gorm.Statement{DB: d.DB}
	stmt.Parse(table)
	return stmt.Schema.PrimaryFieldDBNames
}
