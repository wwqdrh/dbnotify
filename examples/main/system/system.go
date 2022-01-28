package system

import (
	"reflect"

	"gorm.io/gorm"
)

// Base ...
type Base struct{}

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
