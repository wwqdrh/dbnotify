package main

import (
	"context"
	"fmt"

	"github.com/wwqdrh/datamanager"
	"github.com/wwqdrh/datamanager/dialet/postgres"
)

// a standalone application
// 1、load config
// 2、connection the db
// 3、start a http server for action
func main() {
	defer datamanager.Logger.Sync()

	dialet, err := postgres.NewPostgresDialet("postgres://postgres:hui123456@localhost:5432/datamanager?sslmode=disable")
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
	for item := range q {
		fmt.Println(item)
	}
}
