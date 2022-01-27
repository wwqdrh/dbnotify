package main

import (
	datamanager "datamanager/server"
	"log"

	"datamanager/server/base"
	system_model "datamanager/server/model/system"
	"datamanager/server/router"
	"time"

	"github.com/ohko/hst"
)

type backend struct{}

// Start ...
func Start(h *hst.HST, middlewares ...hst.HandlerFunc) *hst.HST {
	datamanager.InitService()

	go func() {
		ss := base.Services()
		for _, s := range ss {
			if err := s.Register(); err != nil {
				panic(s.Name() + " register: " + err.Error())
			}
		}
	}()

	// 启动一个清理过期日志的go程
	go func() {
		for {
			system_model.DBServiceLog.ClearTimeout()
			time.Sleep(time.Hour)
		}
	}()

	if h == nil {
		h = hst.New(nil)
	}

	ms := make([]hst.HandlerFunc, 0, len(middlewares))
	ms = append(ms, middlewares...)

	h.RegisterHandle(ms,
		&backend{},
		&router.Bdatalog{},
	)

	router.FileServer(h)

	return h
}

type Company struct {
	ID      int `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	Name    string
	Age     int
	Address string
	Salary  int
}

func main() {
	// system_model.InitDB("postgres", "host=172.18.3.9 user=postgres password=postgres dbname=postgres port=5435 sslmode=disable TimeZone=Asia/Shanghai")
	system_model.InitDB("postgres", "host=localhost user=postgres password=123456 dbname=postgres port=5432 sslmode=disable TimeZone=Asia/Shanghai")

	h := hst.New(nil)
	Start(h)
	// 初始化datamanager db
	datamanager.InitDB(system_model.DB(), ".", nil, &Company{})
	err := h.ListenHTTP(":8080")
	log.Println("exit:", err)
}
