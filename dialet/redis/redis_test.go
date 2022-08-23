//go:build todotest

package redis

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RedisDialetSuite struct {
	suite.Suite

	mode     string
	endpoint string
	dialet   *RedisDialet
}

func TestRedisDialetSuite(t *testing.T) {
	suite.Run(t, &RedisDialetSuite{mode: os.Getenv("mode"), endpoint: "127.0.0.1:6379"})
}

func (s *RedisDialetSuite) SetupTest() {
	if s.mode != "local" {
		return
	}

	dialet, err := NewRedisDialet("localhost:6379", "")
	require.Nil(s.T(), err)
	s.dialet = dialet
}

func (s *RedisDialetSuite) TearDownTest() {
	if s.dialet != nil {
		err := s.dialet.client.Close()
		require.Nil(s.T(), err)
	}
}

func (s *RedisDialetSuite) TestDialetAddPolicy() {
	if s.mode != "local" {
		s.T().Skip("no local env")
	}

	err := s.dialet.AddPolicy("policy1")
	require.Nil(s.T(), err)
}

func (s *RedisDialetSuite) TestDialetModifyPolicy() {
	if s.mode != "local" {
		s.T().Skip("no local env")
	}
}

func (s *RedisDialetSuite) TestDialetListPolicy() {
	if s.mode != "local" {
		s.T().Skip("no local env")
	}

	err := s.dialet.ListPolicy()
	require.Nil(s.T(), err)
	fmt.Println(s.dialet.policy)
}

func (s *RedisDialetSuite) TestDialetWatch() {
	// 获取数据 需要注意不能使用同一个channel 否则可能造成只有一个地方能获取数据
	if s.mode != "local" {
		s.T().Skip("no local env")
	}

	// 设置一个元素 然后监听 再次更新看监听的部分能否查看到

}
