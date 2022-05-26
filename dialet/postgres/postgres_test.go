package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

var postgresDialet *PostgresDialet

var (
	testDatabaseDDL    = `create table if not exists notes (id serial, created_at timestamp, name varchar(100), note text)`
	testInsert         = `insert into notes values (default, default, 'user1', 'here is a sample note')`
	testInsertTemplate = `insert into notes values (default, default, 'user1' '%s')`
	testUpdate         = `update notes set note = 'here is an updated note' where id=1`
	testUpdateTemplate = `update notes set note = 'i%s' where id=1`
)

// build test table and drop
func dbMain() bool {
	var s string
	if s = os.Getenv("DB_DSN"); s == "" {
		fmt.Println("未设置DB_DSN，跳过测试")
		return false
	}

	var err error
	postgresDialet, err = NewPostgresDialet(s)
	if err != nil {
		fmt.Println(err)
	}

	_, err = postgresDialet.stream.DB().Exec(testDatabaseDDL)
	if err != nil {
		fmt.Println(err)
		return false
	}
	return true
}

// should have a pre-difine table, example the TestMain
func TestAddTablePolicy(t *testing.T) {
	if !dbMain() {
		t.Skip("未初始化db")
	}
	defer func() {
		postgresDialet.Close()
	}()

	if err := postgresDialet.Register("notes"); err != nil {
		t.Fatal(err)
	}
	if err := postgresDialet.UnRegister("notes"); err != nil {
		t.Fatal(err)
	}
}

func TestAddTableAndInsert(t *testing.T) {
	if !dbMain() {
		t.Skip("未初始化db")
	}
	defer func() {
		postgresDialet.Close()
	}()

	if err := postgresDialet.Register("notes"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := postgresDialet.UnRegister("notes"); err != nil {
			t.Fatal(err)
		}
	}()

	if _, err := postgresDialet.stream.DB().Exec(testInsert); err != nil {
		t.Fatal(err)
	}
}

// {"schema":"public","table":"notes","op":1,"id":"14","payload":{"created_at":null,"id":14,"name":"user1","note":"here is a sample note"}}
// {"schema":"public","table":"notes","op":2,"id":"1","payload":{"created_at":null,"id":1,"name":"user1","note":"here is an updated note"},"changes":{"note":"here is a sample note"}}
// {"schema":"public","table":"notes","op":1,"id":"15","payload":{"created_at":null,"id":15,"name":"user1","note":"here is a sample note"}}
func TestAddTableAndInsertAndNotify(t *testing.T) {
	if !dbMain() {
		t.Skip("未初始化db")
	}
	defer func() {
		postgresDialet.Close()
	}()

	if err := postgresDialet.Register("notes"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := postgresDialet.UnRegister("notes"); err != nil {
			t.Fatal(err)
		}
	}()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	q := make(chan string, 1)
	go func() {
		if err := postgresDialet.stream.HandleEvents(ctx, q); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		for item := range q {
			fmt.Println(item)
		}
	}()

	if _, err := postgresDialet.stream.DB().Exec(testInsert); err != nil {
		t.Error(err)
	}
	if _, err := postgresDialet.stream.DB().Exec(testUpdate); err != nil {
		t.Error(err)
	}
	if _, err := postgresDialet.stream.DB().Exec(testInsert); err != nil {
		t.Error(err)
	}
	time.Sleep(3 * time.Second)
}
