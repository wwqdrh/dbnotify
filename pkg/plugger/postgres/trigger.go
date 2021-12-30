package postgres

import (
	"datamanager/pkg/plugger/postgres/model"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

// 触发器
type (
	TriggerType int

	TriggerCore struct {
		db        *gorm.DB
		trigger   map[string]bool
		TableName string      // 表名
		Type      TriggerType // 触发器类型
	}

	ITriggerPolicy interface {
		Initial(ITriggerPolicy) error        // 初始化
		UpdateConfig(map[string]interface{}) // 修改配置的接口
		GetPrefix(ITriggerPolicy) string     // 获取前缀
		GetTriggerName(ITriggerPolicy, string, TriggerType) string
		GetTriggerSql(ITriggerPolicy, TriggerType) string
		GetAllTriggerName(ITriggerPolicy) []string // 获取所有的trigger名字
		GetAllTrigger() string
		BeforeUpdateTrigger() string
		AfterUpdateTrigger() string
		BeforeDeleteTrigger() string
		AfterDeleteTrigger() string
		BeforeInsertTrigger() string
		AfterInsertTrigger() string
		UpdateTrigger() string
		InsertTrigger() string
		DeleteTrigger() string
		TruncateTrigger() string
	}

	DefaultTriggerPolicy struct{}
)

const (
	BEFORE_CREATE TriggerType = iota
	AFTER_CREATE
	BEFORE_DELETE
	AFTER_DELETE
	BEFORE_UPDATE
	AFTER_UPDATE
	UPDATE
	INSERT
	DELETE
	TRUNCATE
	ALL
)

func NewTriggerCore(db *gorm.DB) *TriggerCore {
	res := []*model.PgTrigger{}
	db.Find(&res)

	triggerName := map[string]bool{}
	for _, item := range res {
		triggerName[item.TgName] = true
	}

	return &TriggerCore{
		db:      db,
		trigger: triggerName,
	}
}

func (t *TriggerCore) CreateTriggerDefault(tableName string) []error {
	return t.CreateTrigger(&DefaultTriggerPolicy{}, tableName, []TriggerType{BEFORE_CREATE, AFTER_CREATE, BEFORE_UPDATE, AFTER_UPDATE, BEFORE_DELETE, AFTER_DELETE})
}

// CreateTriggerIfNotExist 如果存在就不创建 不存在就创建
func (t *TriggerCore) CreateTrigger(policy ITriggerPolicy, tableName string, tType []TriggerType) (errs []error) {
	if err := policy.Initial(policy); err != nil {
		errs = append(errs, err)
		return
	}

	triggerSQL := ""

	for _, item := range tType {
		triggerName := policy.GetTriggerName(policy, tableName, item)
		if _, ok := t.trigger[triggerName]; ok {
			errs = append(errs, errors.New(triggerName+"已经存在"))
			continue
		}

		triggerSQL += policy.GetTriggerSql(policy, item)
	}

	if err := t.db.Exec(triggerSQL).Error; err != nil {
		errs = append(errs, err)
	}
	return
}

func (t *TriggerCore) DeleteTrigger(policy ITriggerPolicy, tableName string, tType TriggerType) error {
	triggerName := policy.GetTriggerName(policy, tableName, tType)
	if _, ok := t.trigger[triggerName]; !ok {
		return fmt.Errorf("%s 不存在", triggerName)
	}
	if err := t.db.Where("tgname = ?", triggerName).Delete(&model.PgTrigger{}).Error; err != nil {
		return err
	}
	delete(t.trigger, triggerName)
	return nil
}

////////////////////
// 默认的trigger生成
////////////////////

func (DefaultTriggerPolicy) Initial(this ITriggerPolicy) error {
	return nil
} // 初始化

func (DefaultTriggerPolicy) UpdateConfig(map[string]interface{}) {}

func (DefaultTriggerPolicy) GetPrefix(this ITriggerPolicy) string {
	return "default"
} // 获取前缀

func (DefaultTriggerPolicy) GetTriggerName(this ITriggerPolicy, tableName string, tType TriggerType) string {
	prefix := this.GetPrefix(this) + tableName

	switch tType {
	case BEFORE_CREATE:
		return prefix + "_before_create"
	case AFTER_CREATE:
		return prefix + "_after_create"
	case BEFORE_UPDATE:
		return prefix + "_before_update"
	case AFTER_UPDATE:
		return prefix + "_after_update"
	case BEFORE_DELETE:
		return prefix + "_before_delete"
	case AFTER_DELETE:
		return prefix + "_after_delete"
	case UPDATE:
		return prefix + "_update"
	case DELETE:
		return prefix + "_delete"
	case INSERT:
		return prefix + "_insert"
	case TRUNCATE:
		return prefix + "_truncate"
	case ALL:
		return prefix + "_action_log"
	default:
		return ""
	}
}

func (DefaultTriggerPolicy) GetAllTriggerName(this ITriggerPolicy) []string {
	res := []string{"_before_create", "_after_create", "_before_delete", "_after_delete", "_before_update", "_after_update"}
	prefix := this.GetPrefix(this)
	for i, val := range res {
		res[i] = prefix + val
	}
	return res
} // 获取所有的trigger名字

func (DefaultTriggerPolicy) GetTriggerSql(this ITriggerPolicy, tType TriggerType) string {
	switch tType {
	case BEFORE_CREATE:
		return this.BeforeInsertTrigger()
	case AFTER_CREATE:
		return this.AfterInsertTrigger()
	case BEFORE_UPDATE:
		return this.BeforeUpdateTrigger()
	case AFTER_UPDATE:
		return this.AfterUpdateTrigger()
	case BEFORE_DELETE:
		return this.BeforeDeleteTrigger()
	case AFTER_DELETE:
		return this.AfterDeleteTrigger()
	case UPDATE:
		return this.UpdateTrigger()
	case DELETE:
		return this.DeleteTrigger()
	case INSERT:
		return this.InsertTrigger()
	case ALL:
		return this.GetAllTrigger()
	default:
		return ""
	}
}

func (p DefaultTriggerPolicy) BeforeUpdateTrigger() string {
	return ""
}
func (p DefaultTriggerPolicy) AfterUpdateTrigger() string {
	return ""
}
func (p DefaultTriggerPolicy) BeforeDeleteTrigger() string {
	return ""
}
func (p DefaultTriggerPolicy) AfterDeleteTrigger() string {
	return ""
}
func (p DefaultTriggerPolicy) BeforeInsertTrigger() string {
	return ""
}
func (p DefaultTriggerPolicy) AfterInsertTrigger() string {
	return ""
}
func (p DefaultTriggerPolicy) UpdateTrigger() string   { return "" }
func (p DefaultTriggerPolicy) InsertTrigger() string   { return "" }
func (p DefaultTriggerPolicy) DeleteTrigger() string   { return "" }
func (p DefaultTriggerPolicy) TruncateTrigger() string { return "" }
func (p DefaultTriggerPolicy) GetAllTrigger() string   { return "" }
