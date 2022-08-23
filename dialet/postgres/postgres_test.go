package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	testDatabaseDDL     = `create table if not exists notes_postgres (id serial, created_at timestamp, name varchar(100), note text)`
	testInsert          = `insert into notes_postgres values (default, default, 'user1', 'here is a sample note')`
	testDatabaseDropDDL = `drop table notes_postgres`
	// testInsertTemplate  = `insert into notes_postgres values (default, default, 'user1' '%s')`
	testUpdate = `update notes_postgres set note = 'here is an updated note' where id=1`
	// testUpdateTemplate  = `update notes_postgres set note = 'i%s' where id=1`
)

type PostgresSuite struct {
	suite.Suite

	dial *PostgresDialet
}

func TestPostgresSuite(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		t.Skip("dsn为空")
	}

	dial, err := NewPostgresDialet(dsn)
	require.Nil(t, err)

	suite.Run(t, &PostgresSuite{
		dial: dial,
	})
}

func (s *PostgresSuite) SetupSuite() {
	assert.Nil(s.T(), s.dial.Exec(testDatabaseDDL))
	assert.Nil(s.T(), s.dial.stream.InstallTriggers())
	// assert.Nil(s.T(), s.dial.Register("notes_postgres"))
}

func (s *PostgresSuite) TearDownSuite() {
	assert.Nil(s.T(), s.dial.Exec(testDatabaseDropDDL))
	assert.Nil(s.T(), s.dial.UnRegister("notes_postgres"))
}

func (s *PostgresSuite) TestAddTableAndInsert() {
	require.Nil(s.T(), s.dial.Exec(testInsert))
	time.Sleep(1 * time.Second)
}

// {"schema":"public","table":"notes","op":1,"id":"14","payload":{"created_at":null,"id":14,"name":"user1","note":"here is a sample note"}}
// {"schema":"public","table":"notes","op":2,"id":"1","payload":{"created_at":null,"id":1,"name":"user1","note":"here is an updated note"},"changes":{"note":"here is a sample note"}}
// {"schema":"public","table":"notes","op":1,"id":"15","payload":{"created_at":null,"id":15,"name":"user1","note":"here is a sample note"}}
func (s *PostgresSuite) TestAddTableAndInsertAndNotify() {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	q := make(chan string, 1)
	go func() {
		assert.Nil(s.T(), s.dial.stream.HandleEvents(ctx, q))
	}()
	go func() {
		for item := range q {
			fmt.Println(item)
		}
	}()

	if _, err := s.dial.stream.DB().Exec(testInsert); err != nil {
		s.T().Error(err)
	}
	if _, err := s.dial.stream.DB().Exec(testUpdate); err != nil {
		s.T().Error(err)
	}
	if _, err := s.dial.stream.DB().Exec(testInsert); err != nil {
		s.T().Error(err)
	}
	time.Sleep(5 * time.Second) // wait the event done
}
