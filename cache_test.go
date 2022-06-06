package datamanager

import (
	"context"
	"testing"
	"time"

	"github.com/wwqdrh/datamanager/dialet"
)

type testLog struct {
	schema  string
	table   string
	payload map[string]interface{}
}

func (t *testLog) GetSchema() string {
	return t.schema
}
func (t *testLog) GetTable() string {
	return t.table
}
func (t *testLog) GetType() string {
	return "ddl"
} // 获取日志记录类型 ddl dml
func (t *testLog) GetLabel() string {
	return "insert"
} // 具体标签 insert update delete | alter column, table
func (t *testLog) GetTime() time.Time {
	return time.Now()
} // 获取日志记录时间
func (t *testLog) GetPaylod() map[string]interface{} {
	return t.payload
} // 获取具体的负载对象
func (t *testLog) GetChange() map[string]interface{} {
	return map[string]interface{}{}
}

func TestSimpleRegister(t *testing.T) {
	ch := make(chan dialet.ILogData, 1)
	cacheValue := "valueA"
	ctx, cancel := context.WithCancel(context.TODO())
	r := NewRepo(ch)
	r.Register(&Policy{
		Key:   "cacheA",
		Table: "tableA",
		Field: "*",
		Call: func() interface{} {
			return cacheValue
		},
	})
	go r.Notify(ctx)

	if val, ok := r.GetValue("cacheA").(string); !ok || val != "valueA" {
		t.Error()
	}
	cacheValue = "valueB"
	ch <- new(testLog)
	time.Sleep(1 * time.Second) // 等待缓存更新完成
	if val, ok := r.GetValue("cacheA").(string); !ok || val != "valueB" {
		t.Error()
	}
	cancel()
	time.Sleep(1 * time.Second)
}

func TestUpdateRecord(t *testing.T) {
	fieldValue := "value1"
	lg := testLog{
		schema: "public",
		table:  "table1",
		payload: map[string]interface{}{
			"id":     1,
			"field1": "value1",
			"field2": "value2",
		},
	}

	ch := make(chan dialet.ILogData, 1)
	ctx, cancel := context.WithCancel(context.TODO())
	r := NewRepo(ch)
	r.Register(&Policy{
		Key:   "cacheA",
		Table: "table1",
		Field: "field1",
		Call: func() interface{} {
			return fieldValue
		},
	})
	go r.Notify(ctx)

	if val, ok := r.GetValue("cacheA").(string); !ok || val != "value1" {
		t.Error()
	}
	fieldValue = "value2"
	ch <- &lg
	time.Sleep(1 * time.Second) // 等待缓存更新完成
	if val, ok := r.GetValue("cacheA").(string); !ok || val != "value2" {
		t.Error()
	}
	cancel()
	time.Sleep(1 * time.Second)
}
