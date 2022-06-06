# 环境初始化

```Bash
# redis
docker run -d --name redis6 -v $(pwd)"/redis.conf":/etc/redis/redis.conf -p 6379:6379 redis:6-alpine redis-server /etc/redis/redis.conf

# postgres
```

处理缓存一致性问题

- 缓存预热
- 注册缓存的处理函数
- 启动数据的监听，触发缓存更新操作
- 其他程序读取时直接读取缓存，没有的话触发缓存处理模块(并配置singleflight)获取缓存数据
- 修改数据，直接操作db，缓存中的更新以及删除操作由缓存模块来处理，并且修改时如果存在多次更新，使用时间戳来判断先后顺序，更新最新的数据

```go
// 当notes表发生改变时就会触发缓存更新
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
```