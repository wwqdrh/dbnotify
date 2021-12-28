package postgres

import (
	"fmt"
	"time"
)

// 在连接的dsn中，添加parseTime=true 和loc=Local，此处的local可以换为具体的时区(Asia/Shanghai)
// 使用go-sql-driver来连接Postgres数据库，获取的时区默认是UTC +0的，与本地的东八区是有区别，在业务处理中会出现问题
// 获取Postgres中的日期，是string类型，需要在代码中用time.Parse进行转化

// PostgresConfig Postgres连接的配置管理
type PostgresConfig struct {
	Account  string
	Password string
	Host     string
	DBName   string
	Charset  string // utf8
	// ParseTime bool   // true
	Loc string // Local

	PostgresConnectionPool
}
type PostgresConnectionPool struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
}

// NewPostgresConfig Postgresconfig的构造函数，必选参数account、password、host、port
func NewPostgresConfig(account, password, host, db string) *PostgresConfig {
	return &PostgresConfig{
		Account:  account,
		Password: password,
		Host:     host,
		DBName:   db,
		Charset:  "utf8",
		Loc:      "Local",
		PostgresConnectionPool: PostgresConnectionPool{
			MaxIdleConns:    10,
			MaxOpenConns:    100,
			ConnMaxLifetime: time.Hour,
		},
	}
}

func (c *PostgresConfig) SetDBName(value string) *PostgresConfig {
	c.DBName = value
	return c
}

func (c *PostgresConfig) SetCharset(value string) *PostgresConfig {
	c.DBName = value
	return c
}

func (c *PostgresConfig) SetLoc(value string) *PostgresConfig {
	c.Loc = value
	return c
}

func (c *PostgresConfig) SetMaxIdleConns(num int) *PostgresConfig {
	c.MaxIdleConns = num
	return c
}

func (c *PostgresConfig) SetMaxOpenConns(num int) *PostgresConfig {
	c.MaxOpenConns = num
	return c
}

func (c *PostgresConfig) SetConnMaxLifetime(duration time.Duration) *PostgresConfig {
	c.ConnMaxLifetime = duration
	return c
}

// 根据配置结构体生成对应的dnsstring
// 1、没有指定DBName，一般是用来处理连通性测试，或者应用初始化还没有建立数据库的时候
// 2、指定了DBName 在具体应用实例中
func (c PostgresConfig) DNS() string {
	// postgres://postgres:postgres@postgres/material?sslmode=disable
	var DNS string
	if c.DBName == "" {
		DNS = fmt.Sprintf("postgres://%s:%s@%s/sslmode=disable", c.Account, c.Password, c.Host)
	} else {
		DNS = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", c.Account, c.Password, c.Host, c.DBName)
	}
	return DNS
}

// DryDNS 最简dns配置 主要用于连通性测试 postgre好像也必须得设置数据库
func (c PostgresConfig) DryDNS() string {
	return fmt.Sprintf("postgres://%s:%s@%s?sslmode=disable", c.Account, c.Password, c.Host)
}
