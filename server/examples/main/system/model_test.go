package system

import (
	"encoding/json"
	"log"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	DBMyTest = &MyTest{}
)

type MyTest struct {
	Base
	gorm.Model
	Name    string
	Age     int
	Six     bool
	ClassID uint
	Class   *Class
}
type Class struct {
	gorm.Model
	Name  string
	Level int
}

func init() {
	var err error
	db, err = gorm.Open(postgres.Open("host=localhost user=postgres password=postgres dbname=erp port=5432 sslmode=disable TimeZone=Asia/Shanghai"), nil)
	if err != nil {
		panic(err)
	}
	db = db.Debug()
	db.AutoMigrate(&MyTest{}, &Class{})
}

func (o *MyTest) GetPrimaryName() string {
	return "id"
}

func (o *MyTest) GetPrimaryValue() interface{} {
	return o.ID
}

func (o *MyTest) GetModel() interface{} {
	return &MyTest{}
}

func (o *MyTest) GetSlice() interface{} {
	var l []*MyTest
	return &l
}

func TestModel(t *testing.T) {
	err := DBMyTest.Create(&MyTest{
		Name:    "test-2",
		Age:     10,
		Six:     true,
		ClassID: 1,
		Class:   &Class{Name: "class-1", Level: 1},
	})
	if err != nil {
		t.Error(err)
	}
	err = DBMyTest.Create(&MyTest{
		Name:    "test-3",
		Age:     11,
		ClassID: 1,
	})
	if err != nil {
		t.Error(err)
	}

	cnt, l, err := DBMyTest.Find(&MyTest{}, &Condition{
		Where:    map[string]interface{}{"class_id": 1},
		Preloads: []string{"Class"}})
	if err != nil {
		t.Error(err)
	}
	bs, _ := json.Marshal(l)
	log.Println(cnt, string(bs))
}
