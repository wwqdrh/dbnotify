package model

import (
	"time"

	"datamanager/server/pkg/plugger"

	"datamanager/server/model"
)

var (
	LevelDBDriver  *plugger.LevelDBDriver
	PostgresDriver *plugger.PostgresDriver
)

func init() {
	driver, err := plugger.NewLevelDBDriver(".")
	if err != nil {
		panic(err)
	}

	model.InitDB("postgres", "host=office.zx-tech.net user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	LevelDBDriver = driver
	PostgresDriver = new(plugger.PostgresDriver).InitWithDB(model.DB())

	LogRepoV2 = &LocalLog2{
		db:       LevelDBDriver,
		targetDB: PostgresDriver,
	}
}
func ExampleLocalLog2Write() {
	now := time.Now()
	LogRepoV2.Write("company", []map[string]interface{}{
		{
			"log": map[string]interface{}{
				"data": map[string]interface{}{
					"name": map[string]interface{}{
						"before": "zhangsan",
						"after":  "zangshan",
					},
				},
				"primary": map[string]interface{}{
					"id": map[string]interface{}{
						"before": 3,
						"after":  3,
					},
				},
			},
			"action": "update",
			"time":   &now,
		}, {
			"log": map[string]interface{}{
				"data": map[string]interface{}{
					"name": "张三",
				},
				"primary": map[string]interface{}{
					"id": 6,
				},
			},
			"action": "delete",
			"time":   &now,
		},
	}, 15, 10)

	// Output:
}

func ExampleLocalLog2Search() {
	LogRepoV2.SearchRecordByField("company", "id=7", nil, nil, 0, 0)

	// Output:
}
