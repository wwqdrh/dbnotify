<div align="center">
  <p>
      <pre style="float:center">
 _ .-') _   .-. .-')        .-') _                .-') _
( (  OO) )  \  ( OO )      ( OO ) )              (  OO) )
 \     .'_   ;-----.\  ,--./ ,--,'   .-'),-----. /     '._    ,-.-')     ,------.   ,--.   ,--.
 ,`'--..._)  | .-.  |  |   \ |  |\  ( OO'  .-.  '|'--...__)   |  |OO) ('-| _.---'    \  `.'  /
 |  |  \  '  | '-' /_) |    \|  | ) /   |  | |  |'--.  .--'   |  |  \ (OO|(_\      .-')     /
 |  |   ' |  | .-. `.  |  .     |/  \_) |  |\|  |   |  |      |  |(_/ /  |  '--.  (OO  \   /
 |  |   / :  | |  \  | |  |\    |     \ |  | |  |   |  |     ,|  |_.' \_)|  .--'   |   /  /\_
 |  '--'  /  | '--'  / |  | \   |      `'  '-'  '   |  |    (_|  |      \|  |_)    `-./  /.__)
 `-------'   `------'  `--'  `--'        `-----'    `--'      `--'       `--'        `--'
  </pre>
  </p>
  <p>

  <p align='center'>
方便地<sup><em>dbnotify</em></sup>数据变更通知工具
<br> 
</p>


[![Build Status](https://github.com/wwqdrh/dbnotify/actions/workflows/push.yml/badge.svg)](https://github.com/wwqdrh/dbnotify/actions)
[![codecov](https://codecov.io/gh/wwqdrh/dbnotify/branch/main/graph/badge.svg?token=LC47C9J0XZ)](https://codecov.io/gh/wwqdrh/dbnotify)

  </p>
</div>

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


## 示例

- [缓存一致性](./examples/cacheupdate/README.md)
- [基于http调用获取数据通知](./examples/httpnotify/README.md)

## 使用手册

### 开发环境搭建

创建postgres环境并配置DSN至env文件(创建过程可参考`make env-postgres`)

`make test -e env=.env`

### 安装
<br>

```bash
go install github.com/wwqdrh/datamanager/cmd/dbnotify@latest
```

### 使用
<br>

```bash
dbmonitor -dsn postgres://[用户名]:[密码]@localhost:5432/datamanager\?sslmode=disable
```
<br>

提供http接口进行访问，注册表监听、取消监听
<br>

<img src="./docs/img/示例.png" />
<br>
