package model

type PgTrigger struct {
	TgName string `gorm:"column:tgname"`
	TgType int    `gorm:"column:tgtype"`
}

func (PgTrigger) TableName() string {
	return "pg_trigger"
}
