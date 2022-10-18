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
	"github.com/wwqdrh/datamanager/transport/sqlite"
	"github.com/wwqdrh/logger"
)

var (
	dsn  *string = flag.String("dsn", "", "需要连接的postgresdsn: postgres://[user]:[password]@[host]:[port]/[db]?sslmode=disable") // postgres://postgres:hui123456@localhost:5432/datamanager?sslmode=disable
	port *int    = flag.Int("port", 8000, "用于交互的http端口")
)

var (
	dialet           *postgres.PostgresDialet
	sqlite3transport *sqlite.SqliteTransport
	watcher          *datamanager.Watcher
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
	InitRouter(engine)
	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: engine,
	}

	go func() {
		logger.DefaultLogger.Info(fmt.Sprintf(":%d: is start", *port))
		if err := srv.ListenAndServe(); err != nil {
			logger.DefaultLogger.Error(err.Error())
		}
	}()
	<-ctx.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.DefaultLogger.Error("服务异常退出" + err.Error())
	} else {
		logger.DefaultLogger.Info("服务正常退出")

	}

}

func monitor(ctx context.Context) {
	var err error
	// dialet
	dialet, err = postgres.NewPostgresDialet(*dsn)
	if err != nil {
		logger.DefaultLogger.Error(err.Error())

		return
	}

	if err := dialet.Initial(); err != nil {
		logger.DefaultLogger.Error(err.Error())
		return
	}
	if err := dialet.Register("notes"); err != nil {
		logger.DefaultLogger.Error(err.Error())
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	// watcher
	// TODO 现在的数据只能消费一次
	// watcher = datamanager.NewWatcher(dialet)
	// watcher.Notify(ctx)

	q := make(chan string, 1)
	go func() {
		if err := dialet.Stream().HandleEvents(ctx, q); err != nil {
			logger.DefaultLogger.Error(err.Error())

		}
	}()
	logger.DefaultLogger.Info("start...")

	plaintransport := new(plain.PlainTransport)
	sqlite3transport, err = sqlite.NewSqliteTransport("data.db")
	if err != nil {
		logger.DefaultLogger.Error(err.Error())
	}
	for item := range q {
		plaintransport.Save(item)
		l, err := postgres.NewPostgresLog(item)
		if err != nil {
			logger.DefaultLogger.Error(err.Error())

		}
		if err := sqlite3transport.Save(l); err != nil {
			logger.DefaultLogger.Error(err.Error())

		}
	}
	<-ctx.Done()
}

// a standalone application
// 1、load config
// 2、connection the db
// 3、start a http server for action
func main() {
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
