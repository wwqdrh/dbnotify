package mongodb

import (
	"fmt"

	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongodb的安全策略 是不同的数据库不同的表都有对应的权限

type MongoDBConfig struct {
	Host     string
	Port     int
	Username string
	Password string
}

// MongoDBConnPool mongodb连接池管理
type MongoDBConnPool struct{}

func NewMongoDBConfig(host string, port int, Username, password string) *MongoDBConfig {
	return &MongoDBConfig{
		Host:     host,
		Port:     port,
		Username: Username,
		Password: password,
	}
}

func (c *MongoDBConfig) SetUsername(value string) *MongoDBConfig {
	c.Username = value
	return c
}

func (c *MongoDBConfig) SetPassword(value string) *MongoDBConfig {
	c.Password = value
	return c
}

func (c *MongoDBConfig) DSN() interface{} {
	// collection := client.Database("baz").Collection("qux")
	// 所以不需要指定数据库
	// options.Client().ApplyURI("mongodb://foo:bar@localhost:27017")

	return options.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%d", c.Host, c.Port)).SetAuth(
		options.Credential{
			Username: c.Username,
			Password: c.Password,
		},
	)
}
