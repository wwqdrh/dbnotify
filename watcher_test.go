package datamanager

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/wwqdrh/datamanager/dialet/postgres"
)

var (
	testDropTable   = `drop table if exists notes`
	testDatabaseDDL = `create table notes (id serial, created_at timestamp, note text)`
	insertTemplate  = "insert into notes values (default, default, 'here is a sample note')"
)

type WatcherSuite struct {
	suite.Suite

	dialet *postgres.PostgresDialet
}

func TestWatcherSuite(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip("dsn为空")
	}

	dial, err := postgres.NewPostgresDialet(dsn)
	require.Nil(t, err)

	suite.Run(t, &WatcherSuite{
		dialet: dial,
	})
}

func (s *WatcherSuite) SetupSuite() {
	require.Nil(s.T(), s.dialet.Exec(testDatabaseDDL))
}

func (s *WatcherSuite) TearDownSuite() {
	require.Nil(s.T(), s.dialet.Exec(testDropTable))
	// require.Nil(s.T(), s.dialet.Close()) // close是删除监听的表
}

func (s *WatcherSuite) TestWatcherNotify() {
	watcher := NewWatcher(s.dialet)
	ctx, cancel := context.WithCancel(context.TODO())
	go watcher.Notify(ctx)
	require.Nil(s.T(), s.dialet.Exec(insertTemplate))

	time.Sleep(1 * time.Second) // 避免太快取消notify通知不到
	cancel()
	time.Sleep(1 * time.Second)
}

func (s *WatcherSuite) TestWatcherCallback() {
	ctx, cancel := context.WithCancel(context.TODO())

	engine := func(ctx context.Context) *gin.Engine {
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
	}(ctx)

	engine.POST("/cb", func(ctx *gin.Context) {
		body, err := ioutil.ReadAll(ctx.Request.Body)
		if err != nil {
			ctx.String(500, err.Error())
			return
		}
		fmt.Println(string(body))
		ctx.String(200, "ok")
	})

	watcher := NewWatcher(s.dialet)
	go watcher.Notify(ctx)
	watcher.Register("notes", "http://localhost:8080/cb")

	// do db change
	require.Nil(s.T(), s.dialet.Exec(insertTemplate))
	time.Sleep(1 * time.Second) // 避免太快取消notify通知不到
	cancel()
	time.Sleep(1 * time.Second)
}
