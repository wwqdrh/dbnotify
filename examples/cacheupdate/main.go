package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/wwqdrh/datamanager"
	mydialet "github.com/wwqdrh/datamanager/dialet"
	"github.com/wwqdrh/datamanager/dialet/postgres"
	"github.com/wwqdrh/logger"
)

var (
	dsn *string = flag.String("dsn", "", "需要连接的postgresdsn: postgres://[user]:[password]@[host]:[port]/[db]?sslmode=disable") // postgres://postgres:hui123456@localhost:5432/datamanager?sslmode=disable
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

// a standalone application
// 1、load config
// 2、connection the db
// 3、start a http server for action
func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	ch := monitor(ctx)
	repo := cacheRepo(ch)
	go repo.Notify(ctx)

	// 读取缓存中的数据
	fmt.Println(repo.GetValueV2("notescount"))

	// 修改数据库中的数据
	if _, err := dialet.Stream().DB().Exec("insert into notes values (default, default, 'here is a sample note')"); err != nil {
		panic(err)
	}

	// 读取缓存中的数据判断是否更新
	time.Sleep(3 * time.Second)
	fmt.Println(repo.GetValueV2("notescount"))
}

func monitor(ctx context.Context) chan mydialet.ILogData {
	var err error
	// dialet
	dialet, err = postgres.NewPostgresDialet(*dsn)
	if err != nil {
		logger.DefaultLogger.Error(err.Error())
		return nil
	}

	if err := dialet.Initial(); err != nil {
		logger.DefaultLogger.Panic(err.Error())
	}
	if err := dialet.Register("notes"); err != nil {
		logger.DefaultLogger.Panic(err.Error())
	}

	ctx, cancel := context.WithCancel(context.TODO())

	q := make(chan string, 1)
	go func() {
		if err := dialet.Stream().HandleEvents(ctx, q); err != nil {
			logger.DefaultLogger.Error(err.Error())
		}
	}()

	ch := make(chan mydialet.ILogData, 10)
	go func() {
		defer cancel()
		for item := range q {
			l, err := postgres.NewPostgresLog(item)
			if err != nil {
				logger.DefaultLogger.Error(err.Error())
			}
			ch <- l
		}
		<-ctx.Done()
	}()
	return ch
}

func getRedisClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "123456",
		DB:       0, // default DB,
		// TLSConfig: &tls.Config{},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	} else {
		return client, err
	}
}

func cacheRepo(ch chan mydialet.ILogData) *datamanager.Repo {
	repo := datamanager.NewRepo(ch)
	client, err := getRedisClient()
	if err != nil {
		panic(err)
	}
	repo.InitRedisCache(client)

	repo.Register(&datamanager.Policy{
		Key:   "notescount",
		Table: "notes",
		Field: "*",
		Call: func() interface{} {
			var count int
			row := dialet.Stream().DB().QueryRow("select count(id) from notes")
			err := row.Scan(&count)
			if err != nil {
				return -1
			}
			return count
		},
	})

	return repo
}
