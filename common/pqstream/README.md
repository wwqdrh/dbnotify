启动一个协程，可以有n个监听者注册监听

当有消息出来后将这些消息转发给监听者

```
go get google.golang.org/protobuf

protoc -I common/pqstream/proto/ common/pqstream/proto/pqstream.proto --go_out=plugins=grpc:common/pqstream/proto
```

针对单系统-单数据库场景，这种情况系统启动的时候，数据才有可能修改，因此可以启动时才有stream处理
