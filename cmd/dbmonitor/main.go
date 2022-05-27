package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wwqdrh/datamanager"
	"github.com/wwqdrh/datamanager/dialet/postgres"
	"github.com/wwqdrh/datamanager/transport/plain"
)

var (
	dsn  *string = flag.String("dsn", "", "需要连接的postgresdsn: postgres://[user]:[password]@[host]:[port]/[db]?sslmode=disable") // postgres://postgres:hui123456@localhost:5432/datamanager?sslmode=disable
	port *int    = flag.Int("port", 8000, "用于交互的http端口")
)

var (
	dialet *postgres.PostgresDialet
)

func init() {
	flag.Parse()
	if *dsn == "" {
		flag.Usage()
		os.Exit(0)
	}
}

func server(ctx context.Context) {
	engine := gin.Default()
	engine.GET("/health", func(ctx *gin.Context) {
		ctx.String(200, "ok")
	})

	engine.GET("/register", func(ctx *gin.Context) {
		table := ctx.Query("table")
		if table == "" {
			ctx.String(200, "请传入table")
			return
		}

		if err := dialet.Register(table); err != nil {
			ctx.String(200, err.Error())
		} else {
			ctx.String(200, "注册成功")
		}
	})

	engine.GET("/unregister", func(ctx *gin.Context) {
		table := ctx.Query("table")
		if table == "" {
			ctx.String(200, "请传入table")
			return
		}

		if err := dialet.UnRegister(table); err != nil {
			ctx.String(200, err.Error())
		} else {
			ctx.String(200, "取消成功")
		}
	})

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: engine,
	}

	go func() {
		datamanager.Logger.Info(fmt.Sprintf(":%d: is start", *port))
		if err := srv.ListenAndServe(); err != nil {
			datamanager.Logger.Error(err.Error())
		}
	}()
	<-ctx.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	datamanager.Logger.Info("服务退出")
}

func monitor(ctx context.Context) {
	var err error
	dialet, err = postgres.NewPostgresDialet(*dsn)
	if err != nil {
		datamanager.Logger.Error(err.Error())
		return
	}

	dialet.Initial()
	dialet.Register("notes")

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	q := make(chan string, 1)
	go func() {
		if err := dialet.Stream().HandleEvents(ctx, q); err != nil {
			datamanager.Logger.Error(err.Error())
		}
	}()
	datamanager.Logger.Info("start...")

	transport := new(plain.PlainTransport)

	for item := range q {
		transport.Save(item)
	}
	<-ctx.Done()
}

// a standalone application
// 1、load config
// 2、connection the db
// 3、start a http server for action
func main() {
	defer datamanager.Logger.Sync()

	ctx, cancel := context.WithCancel(context.TODO())
	go server(ctx)
	go monitor(ctx)

	// wait a signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	time.Sleep(3 * time.Second) // wait goroutine quit
}
