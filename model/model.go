package model

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

// Base ...
type Base struct{}

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

// Create 创建数据记录
// o 需要创建的数据
// callbacks 回调
func (Base) Create(o IModel, callbacks ...Callback) error {
	if len(callbacks) == 0 {
		return db.Create(o).Error
	}
	return transcation(db, o, func(tx *gorm.DB) error {
		return tx.Create(o).Error
	}, callbacks...)
}

// Updates 更新记录
// o 需要更新数据的表
// value 需要更新的数据值
// condition 条件
// callbacks 回调
func (Base) Updates(o IModel, value interface{}, condition *Condition, callbacks ...Callback) error {
	fn := func(tx *gorm.DB) error {
		return prepareTx(tx, o, condition, false).Updates(value).Error
	}

	return transcation(db, o, fn, callbacks...)
}

// Delete 删除记录
// o 需要删除数据的表
// condition 条件
// callbacks 回调
func (Base) Delete(o IModel, condition *Condition, callbacks ...Callback) error {
	fn := func(tx *gorm.DB) error {
		v := o.GetModel()
		return prepareTx(tx, o, condition, false).Delete(v).Error
	}

	return transcation(db, o, fn, callbacks...)
}

// Find 查找
// o 查找的表
// condition 条件
// return:
//  1: total
//  2: model slice
//  3: error
func (Base) Find(o IModel, condition *Condition) (int64, interface{}, error) {
	tx := db.Model(o.GetModel())
	offset := 0
	limit := 100

	tx = prepareTx(tx, o, condition, true)

	var cnt int64
	if err := tx.Count(&cnt).Error; err != nil || cnt == 0 {
		return 0, nil, err
	}

	if condition != nil {
		for _, pm := range condition.Preloads {
			if pm == "" {
				continue
			}
			tx = tx.Preload(pm)
		}
		for _, ob := range condition.Orderby {
			if ob.Field == "" {
				continue
			}
			orderby := ob.Field
			if ob.Sort == "desc" {
				orderby += " desc"
			}
			tx = tx.Order(orderby)
		}
		offset = condition.Offset
		if condition.Limit != 0 {
			limit = condition.Limit
		}
	}

	l := o.GetSlice()
	if err := tx.Offset(offset).Limit(limit).Find(l).Error; err != nil {
		return 0, nil, err
	}

	// 不要直接返回一个指针
	rv := reflect.ValueOf(l)
	if rv.Type().Kind() == reflect.Ptr {
		rv = rv.Elem()
		return cnt, rv.Interface(), nil
	}

	return cnt, l, nil
}
