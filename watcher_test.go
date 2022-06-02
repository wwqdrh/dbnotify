package datamanager

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/wwqdrh/datamanager/dialet/postgres"
)

var (
	insertTemplate = "insert into notes values (default, default, 'user1', 'here is a sample note')"
)

func startServer(ctx context.Context) *gin.Engine {
	engine := gin.Default()
	srv := http.Server{
		// handler:
		Handler: engine,
		Addr:    ":8080",
	}
	go func() {
		srv.ListenAndServe()
	}()
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	return engine
}

func TestWatcherNotify(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip()
	}

	dial, err := postgres.NewPostgresDialet(dsn)
	if err != nil {
		t.Fatal(err)
	}

	watcher := NewWatcher(dial)
	ctx, cancel := context.WithCancel(context.TODO())
	go watcher.Notify(ctx)
	db, _ := sql.Open("postgres", dsn)
	if _, err := db.Exec(insertTemplate); err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Second) // 避免太快取消notify通知不到
	cancel()
	time.Sleep(1 * time.Second)
}

func TestWatcherCallback(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip()
	}

	ctx, cancel := context.WithCancel(context.TODO())
	dial, err := postgres.NewPostgresDialet(dsn)
	if err != nil {
		t.Fatal(err)
	}

	engine := startServer(ctx)
	engine.POST("/cb", func(ctx *gin.Context) {
		body, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			ctx.String(500, err.Error())
			return
		}
		fmt.Println(string(body))
		ctx.String(200, "ok")
	})

	watcher := NewWatcher(dial)
	go watcher.Notify(ctx)
	watcher.Register("notes", "http://localhost:8080/cb")

	// do db change
	db, _ := sql.Open("postgres", dsn)
	if _, err := db.Exec(insertTemplate); err != nil {
		t.Error(err)
	}
	time.Sleep(1 * time.Second) // 避免太快取消notify通知不到
	cancel()
	time.Sleep(1 * time.Second)

}
