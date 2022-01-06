package model

import (
	"errors"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// QueryItemParam ...
type QueryItemParam struct {
	Where   *Filter   `json:"where,omitempty"`
	OrderBy []OrderBy `json:"order_by,omitempty"`
	Offset  int       `json:"offset,omitempty"`
	Limit   int       `json:"limit,omitempty"`
}

// Filter ...
type Filter struct {
	And   []*Filter   `json:"and,omitempty"`
	Or    []*Filter   `json:"or,omitempty"`
	Field string      `json:"field,omitempty"`
	Query *QueryParam `json:"query,omitempty"`
}

// QueryParam ...
type QueryParam struct {
	Eq   interface{}   `json:"eq,omitempty"`
	Ne   interface{}   `json:"ne,omitempty"`
	Gt   interface{}   `json:"gt,omitempty"`
	Gte  interface{}   `json:"gte,omitempty"`
	Lt   interface{}   `json:"lt,omitempty"`
	Lte  interface{}   `json:"lte,omitempty"`
	In   []interface{} `json:"in,omitempty"`
	Nin  []interface{} `json:"nin,omitempty"`
	Em   string        `json:"em,omitempty"`
	Like string        `json:"lk,omitempty"`
}

// OrderBy ...
type OrderBy struct {
	Field string `json:"field"`
	Sort  string `json:"sort"`
}

// MarshalToDB ...
func (o *Filter) MarshalToDB(d interface{}) (*gorm.DB, error) {
	if db == nil {
		return nil, errors.New("db not ready")
	}
	tx := db.Model(d)
	if o == nil {
		return tx, nil
	}

	t := reflect.TypeOf(d)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, errors.New("data kind not struct")
	}
	table := tx.NamingStrategy.TableName(t.Name())

	fieldMap := make(map[string]reflect.StructField)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		fieldMap[tx.NamingStrategy.ColumnName(table, f.Name)] = f
	}
	sql, args := o.parse(tx, table, fieldMap)
	if sql != "" {
		tx = tx.Where(sql, args...)
	}

	return tx, nil
}

// MarshalToCondition  ...
func (o *Filter) MarshalToCondition(d interface{}) (*Condition, error) {
	if db == nil {
		return nil, errors.New("db not ready")
	}

	var c Condition

	if o == nil {
		return &c, nil
	}
	tx := db.Model(d)
	t := reflect.TypeOf(d)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, errors.New("data kind not struct")
	}
	table := tx.NamingStrategy.TableName(t.Name())

	fieldMap := make(map[string]reflect.StructField)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		fieldMap[tx.NamingStrategy.ColumnName(table, f.Name)] = f
	}
	sql, args := o.parse(tx, table, fieldMap)
	if sql != "" {
		c.Where = sql
		c.Args = args
	}

	return &c, nil
}

func (o *Filter) parse(tx *gorm.DB, table string, fieldMap map[string]reflect.StructField) (string, []interface{}) {
	if o == nil || tx == nil {
		return "", nil
	}

	if o.Field != "" && o.Query != nil {
		name := tx.NamingStrategy.ColumnName(table, o.Field)
		f, ok := fieldMap[name]
		if !ok {
			return "", nil
		}

		// 两种特殊处理的条件
		if o.Query.Em != "" {
			sql := ""
			if "true" == strings.ToLower(o.Query.Em) {
				sql = name + " is null"
				if f.Type.Kind() == reflect.String {
					sql += " or " + name + " =''"
				}
			} else {
				sql = name + " is not null"
				if f.Type.Kind() == reflect.String {
					sql += " AND " + name + " !=''"
				}
			}

			return sql, nil
		} else if o.Query.Like != "" {
			if f.Type.Kind() == reflect.String {
				return name + " LIKE ?", []interface{}{"%" + o.Query.Like + "%"}
			}
			return "", nil
		}

		sql, arg := o.Query.parse(tx, name)

		if arg != nil {
			return sql, []interface{}{arg}
		}
		return sql, nil
	}

	if len(o.And) > 0 {
		sqls := make([]string, 0, len(o.And))
		args := make([]interface{}, 0, len(o.And))
		for _, f := range o.And {
			sql, arg := f.parse(tx, table, fieldMap)
			if sql != "" {
				sqls = append(sqls, sql)
				args = append(args, arg...)
			}
		}

		if len(sqls) > 0 {
			return "(" + strings.Join(sqls, " AND ") + ")", args
		}
		return "", nil
	}

	if len(o.Or) > 0 {
		sqls := make([]string, 0, len(o.Or))
		args := make([]interface{}, len(o.Or))
		for _, f := range o.Or {
			sql, arg := f.parse(tx, table, fieldMap)
			if sql != "" {
				sqls = append(sqls, sql)
				args = append(args, arg...)
			}
		}

		if len(sqls) > 0 {
			return "(" + strings.Join(sqls, " OR ") + ")", args
		}
		return "", nil
	}

	return "", nil
}

func (o *QueryParam) parse(tx *gorm.DB, name string) (string, interface{}) {
	if o == nil || tx == nil {
		return "", nil
	}

	if o.Eq != nil {
		return name + "=?", o.Eq
	}
	if o.Ne != nil {
		return name + "!=?", o.Ne
	}
	if o.Gt != nil {
		return name + ">?", o.Gt
	}
	if o.Gte != nil {
		return name + ">=?", o.Gte
	}
	if o.Lt != nil {
		return name + "<?", o.Lt
	}
	if o.Lte != nil {
		return name + "<=?", o.Lte
	}
	if o.In != nil {
		return name + "in (?)", o.In
	}
	if o.Nin != nil {
		return name + "not in (?)", o.Nin
	}

	return "", nil
}
