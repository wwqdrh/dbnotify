package plugger

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

type (
	PostgresDriver struct {
		DB *gorm.DB
	}
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

func (d *PostgresDriver) InitWithDB(db *gorm.DB) *PostgresDriver {
	d.DB = db
	return d
}

func (d *PostgresDriver) GetTableName(table interface{}) string {
	if val, ok := table.(string); ok {
		return val
	}
	stmt := &gorm.Statement{DB: d.DB}
	stmt.Parse(table)
	tableName := stmt.Schema.Table
	return tableName
}

func (d *PostgresDriver) GetPrimary(table interface{}) ([]string, error) {
	if val, ok := table.(string); ok {
		return d.GetPrimaryWithName(val)
	}
	stmt := &gorm.Statement{DB: d.DB}
	stmt.Parse(table)
	return stmt.Schema.PrimaryFieldDBNames, nil
}

func (d *PostgresDriver) GetPrimaryWithName(tableName string) ([]string, error) {
	rows, err := d.DB.Raw(`select pg_constraint.conname as pk_name,pg_attribute.attname as colname,pg_type.typname as typename from 
	pg_constraint  inner join pg_class 
	on pg_constraint.conrelid = pg_class.oid 
	inner join pg_attribute on pg_attribute.attrelid = pg_class.oid 
	and  pg_attribute.attnum = pg_constraint.conkey[1]
	inner join pg_type on pg_type.oid = pg_attribute.atttypid
	where pg_class.relname = ? 
	and pg_constraint.contype='p'`, tableName).Rows()
	if err != nil {
		return nil, err
	}

	var pkName, colName, typeName string
	res := []string{}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&pkName, &colName, &typeName)
		res = append(res, colName)
	}
	return res, nil
}

// 创建触发器是否成功
func (d *PostgresDriver) CreateTrigger(sql string, triggerName string) error {
	triggers := d.ListTrigger()
	if _, ok := triggers[triggerName]; ok {
		return errors.New("触发器已经存在")
	}

	if err := d.DB.Exec(sql).Error; err != nil {
		return err
	}
	return nil
}

func (d *PostgresDriver) DeleteTrigger(triggerName string) error {
	triggers := d.ListTrigger()
	if _, ok := triggers[triggerName]; !ok {
		return errors.New("触发器不存在")
	}

	if err := d.DB.Exec("delete from pg_trigger where tgname = ?", triggerName).Error; err != nil {
		return err
	}
	return nil
}

func (d *PostgresDriver) ListTrigger() map[string]bool {
	var res map[string]bool = map[string]bool{}
	var triggerName string

	rows, err := d.DB.Raw("select tgname from pg_trigger").Rows()
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

// ListTable 获取数据库中的所有表名称
func (d *PostgresDriver) ListTable() []string {
	var res []string
	var tableName string

	rows, err := d.DB.Raw("select tablename from pg_tables where schemaname=?", "public").Rows()
	if err != nil {
		return []string{}
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tableName)
		res = append(res, tableName)
	}

	return res
}

// ListTableField: 获取表的字段
func (d *PostgresDriver) ListTableField(table interface{}) []string {
	var res []string
	var columnName string

	tableName := d.GetTableName(table)
	rows, err := d.DB.Raw("select column_name from information_schema.columns where table_schema='public' and table_name=?;", tableName).Rows()
	if err != nil {
		return []string{}
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&columnName)
		res = append(res, columnName)
	}
	return res
}
