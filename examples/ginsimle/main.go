package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wwqdrh/datamanager"
	"github.com/wwqdrh/datamanager/runtime"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func main() {
	engine := gin.Default()

	srv := http.Server{
		Handler: engine,
		Addr:    ":8090",
	}

	adapter := GinHanlderAdapter{
		engine: engine,
	}

	db, err := getdb("host=localhost user=postgres password=123456 dbname=hui_dm_test2 port=5432 sslmode=disable TimeZone=Asia/Shanghai")
	if err != nil {
		log.Fatal(err)
	}

	datamanager.Initial(adapter, func(rc *runtime.RuntimeConfig) {
		rc.LogDataPath = "./version"
		rc.MinLogNum = 10
		rc.ReadPolicy = "notify"
		rc.WritePolicy = "leveldb"
		rc.RegisterTable = []datamanager.TablePolicy{
			{
				Table:     Company{},
				RelaField: "id",
				Relations: "company_rela.company_id",
			}, {
				Table:     &CompanyRela{},
				RelaField: "company_id",
				Relations: "company.id",
			},
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	datamanager.Start(db, ctx)
	srv.ListenAndServe()
	cancel()
}

func getdb(dbpath string) (*gorm.DB, error) {
	d := &postgres.Dialector{Config: &postgres.Config{DSN: dbpath}}
	return gorm.Open(d, &gorm.Config{NamingStrategy: &schema.NamingStrategy{SingularTable: true}, DisableForeignKeyConstraintWhenMigrating: true})
}
