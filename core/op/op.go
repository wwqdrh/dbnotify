package op

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type IOperator interface {
	// Exec()
	Initatial() error // 初始化
	ExecSQL() string
	// Rever()
	ReverSQL() string
}

// 需要一个通用的解析语句 能够解析出表名 字段 条件语句各种

type BaseOperator struct {
	rawSql    string            // 原始sql语句
	dest      map[string]string // update 要修改的值
	fields    []string          // 添加 字段
	values    [][]string        // 添加 行的值
	condition map[string]string // 条件语句
	tableName string            // 操作的数据表
}

func NewBaseOperator(rawSql string) (*BaseOperator, error) {
	op := &BaseOperator{
		rawSql: rawSql,
	}
	if err := op.Parse(); err != nil {
		return nil, err
	}
	return op, nil
}

// 解析语句 update delete insert
// UPDATE "company" SET "name"="new_name" WHERE id = 1
func (o *BaseOperator) Parse() error {
	sqlParts := regexp.MustCompile(`\S+`).FindAllString(o.rawSql, -1)
	switch strings.TrimSpace(strings.ToLower(sqlParts[0])) {
	case "update":
		return o._parseUpdateSql(sqlParts)
	case "delete":
		return o._parseDeleteSql(sqlParts)
	case "insert":
		return o._parseInsertSql(sqlParts)
	default:
		return errors.New("sql解析失败，暂时只支持update、delete、insert语句")
	}
}

// "company" SET "name"="new_name" WHERE id = 1
// [0]=>表名 set之后目标值 key=value where之后 condition
func (o *BaseOperator) _parseUpdateSql(sqlParts []string) error {
	var (
		tableName string
		destValue map[string]string = map[string]string{}
		condValue map[string]string = map[string]string{}
	)

	state := 0 // 状态机
	for _, item := range sqlParts[1:] {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if state == 0 {
			tableName = item
			state = 1
		} else if state == 1 {
			if strings.ToLower(item) != "set" {
				state = -1
				break
			}
			state = 2
		} else if state == 2 {
			if strings.ToLower(item) == "where" {
				state = 3
				continue
			}
			t := strings.Split(item, "=")
			if len(t) != 2 {
				state = -1
				break
			}
			destValue[t[0]] = t[1]
		} else if state == 3 {
			t := strings.Split(item, "=")
			if len(t) != 2 {
				state = -1
				break
			}
			condValue[t[0]] = t[1]
		}
	}
	if state == -1 || len(condValue) == 0 || len(destValue) == 0 || tableName == "" {
		// 没有where或者没有set或者没有tablename
		return fmt.Errorf("update sql解析失败: %s", o.rawSql)
	}

	o.tableName = tableName
	o.dest = destValue
	o.condition = condValue
	return nil
}

// DELETE FROM table_name WHERE some_column=some_value;
func (o *BaseOperator) _parseDeleteSql(sqlParts []string) error {
	var (
		tableName string
		condValue map[string]string = map[string]string{}
	)

	state := 0
	for _, item := range sqlParts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if state == 0 {
			if strings.ToLower(item) == "delete" {
				state = 1
				continue
			}
			state = -1
			break
		} else if state == 1 {
			if strings.ToLower(item) == "from" {
				state = 2
				continue
			}
			state = -1
			break
		} else if state == 2 {
			tableName = item
			state = 3
		} else if state == 3 {
			if strings.ToLower(item) == "where" {
				state = 4
				continue
			}
			state = -1
			break
		} else if state == 4 {
			t := strings.Split(item, "=")
			if len(t) != 2 {
				state = -1
				break
			}
			condValue[t[0]] = t[1]
		}
	}
	if state == -1 || tableName == "" || len(condValue) == 0 {
		return fmt.Errorf("delete sql解析失败: %s", o.rawSql)
	}
	o.tableName = tableName
	o.condition = condValue

	return nil
}

// _parseDeleteSql2 使用正则表达式进行解析
//  DELETE FROM table_name WHERE some_column=some_value;
// func (o *BaseOperator) _parseDeleteSql2(sqlParts []string) error {

// }

// INSERT INTO COMPANY (name, age, address, salary) VALUES
// ('Mark', 25, 'Rich-Mond ', 65000.00 ),
// ('David', 27, 'Texas', 85000.00);
func (o *BaseOperator) _parseInsertSql(sqlParts []string) (err error) {
	state := 0
	defer func() {
		if state == -1 {
			err = fmt.Errorf("insert解析失败: %s", o.rawSql)
		} else {
			err = nil
		}
	}()

	var (
		tableName string
		fields    []string
		values    [][]string
	)

	if strings.ToLower(strings.TrimSpace(sqlParts[0])+" "+strings.TrimSpace(sqlParts[1])) != "insert into" {
		state = -1
		return
	}

	fieldsStr := ""
	valueStr := ""
	for _, item := range sqlParts[2:] {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if state == 0 {
			tableName = item
			state = 1
		} else if state == 1 {
			if item[0] != '(' {
				state = -1
				return
			}
			fieldsStr += item
			state = 2
		} else if state == 2 {
			fieldsStr += item
			if item[len(item)-1] == ')' {
				state = 3
			}
		} else if state == 3 {
			if strings.ToLower(item) != "values" {
				state = -1
				return
			}
			state = 4
		} else if state == 4 {
			if item[0] != '(' {
				state = -1
				return
			}
			valueStr += item
			state = 5
		} else if state == 5 {
			valueStr += item
			if item[len(item)-1] == ';' {
				break
			}
		}
	}

	fields = strings.Split(fieldsStr[1:len(fieldsStr)-1], ",")

	for _, item := range regexp.MustCompile(`\(.*?\)`).FindAllString(valueStr, -1) {
		values = append(values, strings.Split(item[1:len(item)-1], ","))
	}

	o.tableName = tableName
	o.fields = fields
	o.values = values
	return nil
}
