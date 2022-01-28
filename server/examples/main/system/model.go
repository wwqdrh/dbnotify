package system

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	db     *gorm.DB
	dbtype string
)

// IModel ...
type IModel interface {
	GetPrimaryName() string       // 获取主键名称
	GetPrimaryValue() interface{} // 获取主键值
	GetModel() interface{}        // 获取一个新的数据接收变量
	GetSlice() interface{}        // 获取一个新的数据接收切片
}

// IQuery ...
type IQuery interface {
	QueryData(c *Condition) (int64, interface{}, error)
}

// Callback ...
type Callback func(tx *gorm.DB, in IModel) error

// Condition ...
type Condition struct {
	Where    interface{}
	Args     []interface{}
	Offset   int
	Limit    int
	Orderby  []Orderby
	Preloads []string
}

// Orderby ...
type Orderby struct {
	Field string
	Sort  string
}

// InitDB ...
func InitDB(driver string, dbpath string, models ...interface{}) (err error) {
	var d gorm.Dialector
	driver = strings.ToLower(driver)
	switch driver {
	default:
		return errors.New("Unsupport db driver")
	case "postgres":
		d = &postgres.Dialector{Config: &postgres.Config{DSN: dbpath}}
	case "mysql":
		d = &mysql.Dialector{Config: &mysql.Config{DSN: dbpath}}
	case "sqlite3":
		d = &sqlite.Dialector{DSN: dbpath}
		os.MkdirAll(filepath.Dir(dbpath), 0755)
	}
	db, err = gorm.Open(d, &gorm.Config{NamingStrategy: &schema.NamingStrategy{SingularTable: true}, DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		return err
	}
	if os.Getenv("DB_DEBUG") == "true" {
		db = db.Debug()
	}
	tx, _ := db.DB()
	if tx != nil {
		tx.SetMaxIdleConns(5)
		tx.SetMaxOpenConns(20)
	}
	err = db.AutoMigrate(&ServiceLog{})
	if err != nil {
		return err
	}
	return db.AutoMigrate(models...)
}

// DB ...
func DB() *gorm.DB {
	return db
}

// SetLogWriter ...
func SetLogWriter(w logger.Writer, lv int) {
	if db == nil {
		return
	}

	db.Logger = logger.New(w, logger.Config{LogLevel: logger.LogLevel(lv)})
}

// Raw ...
func Raw(sql string, values ...interface{}) *gorm.DB {
	if db == nil {
		return nil
	}
	return db.Raw(sql, values...)
}

// TableName ...
func TableName(in interface{}) string {
	if name, ok := in.(string); ok && name != "" {
		return db.NamingStrategy.TableName(name)
	}

	modelType := reflect.ValueOf(in).Type()
	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	if modelType.Kind() != reflect.Struct {
		return ""
	}

	return db.NamingStrategy.TableName(modelType.Name())
}

// transcation 事务执行
// db 未开启事务的db句柄
// o 执行事务的数据
// fn 执行的方法
// callbacks fn执行前和执行后的回调：第一个为before回调，其他为after回调
func transcation(db *gorm.DB, o IModel, fn func(tx *gorm.DB) error, callbacks ...Callback) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if len(callbacks) > 0 {
			before := callbacks[0]
			if before != nil {
				if err := before(tx, o); err != nil {
					return err
				}
			}
		}
		if err := fn(tx); err != nil {
			return err
		}
		if len(callbacks) > 0 {
			for _, callback := range callbacks[1:] {
				if callback == nil {
					continue
				}
				if err := callback(tx, o); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func prepareTx(tx *gorm.DB, o IModel, condition *Condition, checkZero bool) *gorm.DB {
	tx = tx.Model(o.GetModel())
	if condition != nil && condition.Where != nil {
		tx = tx.Where(condition.Where, condition.Args...)
	} else {
		primary := o.GetPrimaryName()
		value := o.GetPrimaryValue()
		if checkZero {
			vv := reflect.ValueOf(value)
			if vv.IsValid() || vv.IsZero() {
				return tx
			}
		}
		tx = tx.Where(primary+"=?", value)
	}
	return tx
}
