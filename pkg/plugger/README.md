## 说明

感觉插件的启动、配置、初始数据的填充都在代码中实现不太好，不仅是有些难以实现，还有就是像这种数据应该是作为容器编排中初始化的步骤才符合应用的运行逻辑

所以这里的所有连接管理都在现有数据的情况下测试数据功能等，不测试自动生成数据的步骤,

账号的生成，权限分配的测试不强求(主要是因为有些不太好实现，可以先占位)

连接器管理

Mysql、Redis、MongoDB、Etcd 等等三方服务的连接器管理

插件的初始化

## redis

docker pull redis:6

docker run -d --name redis6 -p 6379:6379 redis --requirepass "123456"

## mongodb

除了运行容器外 需要使用 admin 账号进入容器并创建对应的账号信息

```Shell
docker run -d --name mongo4 -p 27107:27107 mongo:4

docker exec -it  mongo4  mongo admin  // 超级管理员进入

db.createUser({ user: 'admin', pwd: '123456', roles: [ { role: "userAdminAnyDatabase", db: "admin" } ] }); // 创建管理员

db.auth("admin","123456"); // 认证

db.createUser({ user: 'user', pwd: '123456', roles: [ { role: "readWrite", db: "app" } ] }); // 创建密码、用户、数据库

db.auth("user","123456");

use app

db.test.save({name:"zhangsan"});

db.test.find();
```
