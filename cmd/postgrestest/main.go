package main

import (
	"context"
	"datamanager"
	"datamanager/common/controller"
	"datamanager/pkg/plugger/postgres"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

type Company struct {
	ID      int `gorm:"PRIMARY_KEY;AUTO_INCREMENT"`
	Name    string
	Age     int
	Address string
	Salary  int
}

func (Company) TableName() string {
	return "company"
}

func main() {
	// 数据库初始化操作 后面可以修改句柄创建的方法
	errs := datamanager.Register(&postgres.PostgresConfig{
		Account:  "postgres",
		Password: "postgres",
		Host:     "office.zx-tech.net:5433",
		DBName:   "hui_dm_test",
	}, &Company{})
	if len(errs) > 0 {
		fmt.Println(errs)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		time.Sleep(1 * time.Second)
	}()
	ServerStart(ctx)
}

func ServerStart(appCtx context.Context) {
	srv := &http.Server{
		Handler: appRouter(),
		Addr:    ":8080",
	}

	go func() {
		// 服务连接
		fmt.Println("https://localhost:8080")
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("shutdown... %s", err)
		}

	}()

	// 等待中断信号以优雅地关闭服务器（设置 5 秒的超时时间）
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-quit:
		log.Println("Shutdown Server ...")
	case <-appCtx.Done():
		log.Println("context Shutdown Server ...")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server Shutdown:", err)
	}
	log.Println("Server exiting")
}

func appRouter() *gin.Engine {
	r := gin.Default()

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"hello": "world"})
	})

	r.GET("/table/list", controller.ListTable)
	r.GET("/table/history", controller.ListHistoryByName)
	r.POST("/table/modify/time", controller.ModifyTableTime)
	r.POST("/table/modify/fields", controller.ModifyInsenseField)
	r.GET("/table/history/byfield", controller.ListHistoryByField)

	return r
}
