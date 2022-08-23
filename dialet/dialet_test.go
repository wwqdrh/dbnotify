//go:build todotest

package dialet

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/wwqdrh/datamanager/dialet/postgres"
)

var testConnectionString = "postgres://localhost?sslmode=disable"
var testConnectionStringTemplate = "postgres://localhost/%s?sslmode=disable"

var (
	testDatabaseDDL    = `create table notes (id serial, created_at timestamp, note text)`
	testInsert         = `insert into notes values (default, default, 'here is a sample note')`
	testInsertTemplate = `insert into notes values (default, default, '%s')`
	testUpdate         = `update notes set note = 'here is an updated note' where id=1`
	testUpdateTemplate = `update notes set note = 'i%s' where id=1`
)

func init() {
	if s := os.Getenv("PQSTREAM_TEST_DB_URL"); s != "" {
		testConnectionString = s
	}
	if s := os.Getenv("PQSTREAM_TEST_DB_TMPL_URL"); s != "" {
		testConnectionStringTemplate = s
	}
}

func TestPostgresDialet(t *testing.T) {
	if os.Getenv("LOCAL") == "" {
		t.Skip("no local enviroment")
	}

	post, err := postgres.NewPostgresDialet(testConnectionString)
	if err != nil {
		t.Fatal(err)
	}

	for item := range post.Watch(context.TODO()) {
		if val, ok := item.(ILogData); ok {
			fmt.Println(val.GetLabel())
		}
	}
}
