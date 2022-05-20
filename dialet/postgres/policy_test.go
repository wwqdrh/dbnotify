package postgres

import (
	"fmt"
	"os"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestMain(m *testing.M) {
	if os.Getenv("mode") != "local" {
		fmt.Println("非local环境不做数据库测试")
		return
	}

	if dsn := os.Getenv("dsn"); dsn == "" {
		fmt.Println("未设置dsn")
		return
	} else {
		var err error
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			fmt.Println("数据库连接失败")
		} else {
			m.Run()
		}
	}
}

func TestMigratePolicy(t *testing.T) {
	PolicyName = "_policy"
	if err := (Policy{}).Migrate(); err != nil {
		t.Fatal(err)
	}

	if _, err := new(Policy).Save(); err == nil {
		t.Error("validate未生效")
	}
	if _, err := (&Policy{TableName: "table1"}).Save(); err != nil {
		t.Error("创建数据表失败")
	}
	if err := (&Policy{}).DeleteByTableName("table1"); err != nil {
		t.Error("删除数据表失败")
	}
}
