package system

import (
	"time"
)

// ...
var (
	DBServiceLog = &ServiceLog{}
	logtimeout   = time.Hour * 24 * 30
)

// ServiceLog ...
type ServiceLog struct {
	Base
	ID        uint `gorm:"primary_key"`
	CreatedAt time.Time
	Service   string `gorm:"index"`
	Runner    string `gorm:"index"`
	Title     string `gorm:"index"`
	Key       string
	Success   bool
	Reason    string
}

// GetPrimaryName ...
func (o *ServiceLog) GetPrimaryName() string {
	return "id"
}

// GetPrimaryValue ...
func (o *ServiceLog) GetPrimaryValue() interface{} {
	return o.ID
}

// GetModel ...
func (o *ServiceLog) GetModel() interface{} {
	return &ServiceLog{}
}

// GetSlice ...
func (o *ServiceLog) GetSlice() interface{} {
	var l []*ServiceLog
	return &l
}

// ClearTimeout ...
func (ServiceLog) ClearTimeout() {
	if db == nil {
		return
	}
	var o ServiceLog
	db.Where("created_at < ?", time.Now().Add(logtimeout)).Delete(&o)
}
