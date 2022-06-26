package postgres

import (
	"os"
	"testing"
)

func TestMigratePolicy(t *testing.T) {
	if os.Getenv("LOCAL") == "" {
		t.Skip("no local enviroment")
	}

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
