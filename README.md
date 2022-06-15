<p align='center'>
  <pre style="float:left;"> _ .-') _     ('-.     .-') _      ('-.     _   .-')      ('-.         .-') _    ('-.                   ('-.  _  .-')   
( (  OO) )   ( OO ).-.(  OO) )    ( OO ).-.( '.( OO )_   ( OO ).-.    ( OO ) )  ( OO ).-.             _(  OO)( \( -O )  
 \     .'_   / . --. //     '._   / . --. / ,--.   ,--.) / . --. /,--./ ,--,'   / . --. /  ,----.    (,------.,------.  
 ,`'--..._)  | \-.  \ |'--...__)  | \-.  \  |   `.'   |  | \-.  \ |   \ |  |\   | \-.  \  '  .-./-')  |  .---'|   /`. ' 
 |  |  \  '.-'-'  |  |'--.  .--'.-'-'  |  | |         |.-'-'  |  ||    \|  | ).-'-'  |  | |  |_( O- ) |  |    |  /  | | 
 |  |   ' | \| |_.'  |   |  |    \| |_.'  | |  |'.'|  | \| |_.'  ||  .     |/  \| |_.'  | |  | .--, \(|  '--. |  |_.' | 
 |  |   / :  |  .-.  |   |  |     |  .-.  | |  |   |  |  |  .-.  ||  |\    |    |  .-.  |(|  | '. (_/ |  .--' |  .  '.' 
 |  '--'  /  |  | |  |   |  |     |  | |  | |  |   |  |  |  | |  ||  | \   |    |  | |  | |  '--'  |  |  `---.|  |\  \  
 `-------'   `--' `--'   `--'     `--' `--' `--'   `--'  `--' `--'`--'  `--'    `--' `--'  `------'   `------'`--' '--' </pre>
</p>

<p align='center'>
方便地<sup><em>Datamanager</em></sup> 数据库管理工具
<br> 
</p>

<br>

## 背景

人工使用系统过程中，可能出现数据误修改、误删除等。后台在创建、修改、删除等操作时创建一个历史版本的数据，记录操作时间、操作类型、操作的表 ID、操作的数据前后具体情况。

基于postgres的逻辑复制原理，该工具作为一个逻辑从库，支持用户自定义数据变更后的事件。例如

- 保存在数据库中作为数据备份
- 监听指定表、字段的修改触发事件

## 特性

- 🗂 数据变更通知
- 📦 缓存一致性解决方案
- 🗂 支持postgres

## 使用手册
### 安装
<br>

*克隆到本地*

```bash
git clone https://github.com/wwqdrh/datamanager.git

// GOOS指定目标操作系统 GOARCH指定cpu指令集
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build cmd/dbmonitor/.
```
<br>

*直接安装*

```bash
go install github.com/wwqdrh/datamanager/cmd/dbmonitor@latest
```

### 使用
<br>

```bash
dbmonitor -dsn postgres://[用户名]:[密码]@localhost:5432/datamanager\?sslmode=disable
```
<br>

提供http接口进行访问，注册表监听、取消监听
<br>

<img src="./assets/img/示例.png" />
<br>

### 示例

- [缓存一致性](./examples/cacheupdate/README.md)
- [基于http调用获取数据通知](./examples/httpnotify/README.md)