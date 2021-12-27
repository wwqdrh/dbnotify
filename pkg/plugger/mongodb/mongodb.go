package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBDriver struct {
	clientTemp *mongo.Client
	config     *MongoDBConfig
	Client     *mongo.Client
}

func NewMongoDBDriver(config *MongoDBConfig) (*MongoDBDriver, error) {
	driver := &MongoDBDriver{
		config: config,
	}
	if !driver.Ping() {
		return nil, errors.New("mongodb连接失败")
	}
	return driver, nil
}

func (d *MongoDBDriver) Ping() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	var err error
	var client *mongo.Client
	if d.clientTemp == nil {
		client, err = mongo.Connect(ctx, d.config.DSN().(*options.ClientOptions))
		if err != nil {
			return false
		}
	} else {
		client = d.clientTemp
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel2()
	err = client.Ping(ctx2, nil)
	if err != nil {
		// log.Println(err)
		d.clientTemp = nil
	} else {
		d.clientTemp = client
	}
	return err == nil
}

func (d *MongoDBDriver) Initalial() error {
	if d.clientTemp == nil {
		if !d.Ping() {
			return errors.New("mongodb初始化失败")
		}
	}
	d.Client = d.clientTemp
	return nil
}

// 从连接池中获取连接
func (d *MongoDBDriver) Connection() interface{} {
	return nil
}

// Close 关闭连接 做连接管理等
func (d *MongoDBDriver) Close() error {
	err := d.Client.Disconnect(context.TODO())
	if err != nil {
		return err
	}
	fmt.Println("connection to mongoDB closed")
	return nil
}
