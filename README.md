# 数据管理器

## 背景

人工使用系统过程中，可能出现数据误修改、误删除等。后台在创建、修改、删除等操作时创建一个历史版本的数据，记录操作时间、操作类型、操作的表 ID、操作的数据 ID、操作人、数据明细。有了历史数据，则可以在发现异常的时候进行数据恢复，提供一个通用的数据恢复方法。此需求暂时主要针对 postgres 数据库的数据记录历史信息，数据历史记录在 nosql 数据库中，需要提供一个集成的查看操作界面。

## 设计

·数据库触发器：用于获取数据操作动作和数据明细。
·操作临时记录表：用于临时存储触发器获取到的数据。
·临时数据转存：线程池调度执行器，从临时数据中提取相关信息，设定过期时间（默认一个月，每个表可单独配置）和保留数量（默认最后 10 条，每个表可单独配置，即使过期也要留存），存入 nosql 数据库并删除临时记录；注意：并不是所有的修改操作都需要记录，配置忽略字段组（如果只是修改了组内的字段，则忽略掉这个操作记录）。
·历史数据清理：定时从 nosql 数据库删除过期数据（需满足两个策略，时间过期，最后保留量）。
·数据恢复：用历史版本的数据修改指定的字段。
·配置管理：查看开启了数据版本的表和各表相关配置，提供必要的搜索功能，并可以修改配置；注意：操作人字段需要配置，也可能没有，数据 ID 需要获取表的主键字段。
·数据版本管理：查看和管理历史版本，提供必要的搜索功能，并可以恢复数据。

·参考https://git.zx-tech.net/ljhua/backend 项目，提供相应的路由注册方法，并将界面集成到 go 代码中（go-bindata）。

## 功能点

1、提供一种机制作为数据的快照 提供查询 恢复等操作
2、提供管理界面
3、作为 backend 接入到服务中
4、以数据表的维度进行策略的定制等

## 参考

1、存储快照的实现
[COW、ROW 实现](https://juejin.cn/post/6844903639417372685)

2、使用表的触发器

单表的情况

- 修改: 需要存储修改前的值，然后加上版本号
- 删除: 需要存储目标删除的整段记录，需要修复则进行添加，(但是主键 id 自行指定是否可行?)
- 添加: 要记录添加了 id 值，需要修复则进行删除对应的记录行

多表的情况

- 修改: 多个表无关联那么与单表一样不过是添加了集合，如果多个表有关系(一对一 一对多 多对多)，只要修改整体是一致的那么数据之间不会产生影响
- 删除: 多个表之间的关联性 可能会有数据处理的顺序问题
- 添加: 类似

> 分析知道只要整体的操作满足原子性，那么整体的就不会有问题，只需要注册一个语句的正向以及反向操作即可实现回滚

## 工作进度设计

在数据库的操作之间加一个中间件要么是 gorm 的中间件要么是表上的触发器，将原始的 sql 解析构造逆向 sql 语句并且添加上版本号，存储到某个版本数据存储库中，然后版本的处理是受到配置的策略进行处理的(包括过期策略、以及前面的触发哪些字段是需要监听的配置)， 提供依照版本库中的数据进行临时表创建以及修复，供给查看，历史数据的删除可以使用惰性删除，每次写完成的时候，查看有哪些是过期了的然后对过期数据进行删除。

### 表的注册以及反向操作的实现以及测试

因为其实整体的数据操作其实查询请求量不会很大，写倒是会很多，暂时就先用 sqlite3 实现

1、原始字符串以及反向的注册

postgres 测试数据库的搭建，用线上的吧，方便

```SQL
CREATE TABLE COMPANY(
   ID serial PRIMARY KEY,
   NAME           TEXT    NOT NULL,
   AGE            INT     NOT NULL,
   ADDRESS        CHAR(50),
   SALARY         REAL
);

INSERT INTO COMPANY (name, age, address, salary) VALUES
('Mark', 25, 'Rich-Mond ', 65000.00 ),
('David', 27, 'Texas', 85000.00);
```

查看 gorm 是否有对应的 api，能够方便的提供所需要的字段，比如操作的什么表，更新的什么字段、历史值是多少，如果没有的话
就只能够自己实现对 sql 语句的解析(向这种需要考虑的东西很多)

act.Schema 表中包含的字段返回
act.Dest.data 目标字段以及目标值的映射

但是没有原生 sql 的解析操作，好像只能自己实现, 不过还是能够节省操作，首先构建 model，解析出 table 字段以及值 条件等等，然后再去构造，不过如果所需要的字段都解析完了 其实似乎也不需要再次去构造了

构造完成数据的反向操作之后如何查看某个版本的历史数据构成？有了现在的数据，有了回退的 sql 语句，需要显示的粒度是某个表的状态(select \* from [table]), 创建临时表之后在临时表进行回滚操作，读出数据后再把临时表删除(虽然在磁盘空间中频繁操作表应该不太好，但是暂时对于 sql 的处理等暂时没有更好的方法，还是的依靠数据库原生的数据操作能力)

另外数据的改动是需要无感的 也就是说传入 model 之后 其他的 crud 操作不能发生改变，要么在表中添加触发器，要么在模型上动态添加中间件，不过似乎不太可行(因为用户引入还是只会引入它原先定义的模型 struct，新建的 struct 其实两个对象是不一样)

根据 sql 解析构造反向的 sql 似乎依靠 gorm 无法很好实现 只能自己解析

然后为 gorm.DB 添加中间件，在数据执行前后打印消

```golang
// begin transaction
BeforeSave
BeforeCreate
// save before associations
// insert into database
// save after associations
AfterCreate
AfterSave
// commit or rollback transaction

// 有一个问题就是这个hooks是只能针对模型的粒度，

// 需要使用callback， db.Callback().[action].[before|after].register(name, func)  这些就是全局处理的函数
```

2、表的策略设计，控制粒度的实现

> 策略数据库(sqlite3)

需要能够有哪些字段能够触发数据快照记录操作的设置 也就是说将模型引入之后需要有一个初始化操作，表中的记录字段包括: 表名、能够触发的字段(对那些字段产生了影响，比如 update 中的字段，如果是 insert、delete 的话则是默认会记录整个记录的操作)、过期时间的设置字段。

存储的话可以暂时就使用 sqlite 数据库(简化，平台依赖度低)

> 版本数据库(sqlite3)

其实也可以使用 sqlite3 进行存储，甚至 version 很可能会有很多操作，
可能还需要涉及到分表的操作之类。然后失效策略就使用懒惰删除，在查询的时候顺便对过期的历史记录进行删除

每一次操作都会创建版本数据库数据，包括版本号，正向执行的 sql，反向执行的 sql(留着，可以用来检查正反向的 sql 构建是否有问题)，并且每个版本都是增量的存储 sql 结构，其实跟 redis 的 aof 快照机制类似，都是存储执行的 sql 语句

然后当需要查看某个版本的数据情况的时候，会给版本号，会给表名，页数、分页之类的东西，根据版本号查询到目前为止的所有逆向 sql 操作，

(
1、新建一个临时表，完全一样，然后在表中插入数据模拟然后读取删除这个临时表(但是这个表如果很大肯定是会有性能影响的)
2、在原来的上面进行操作然后在进行恢复，这个肯定是需要添加事务，不能对其他对这个数据进行操作的请求时产生影响
)

有一个问题，现在的设计方案，所有的数据表的 version 都在一条线上，肯定不对，是需要拆分成每个表一个粒度的，多个表之间是可能相互影响。对于互相产生影响的情况下，可以使用一个全局唯一的 id，如果某几个表之间有相同的 id，并且需要回退到这个版本，那么这些都必须回退到这，回退之后不能直接删除，而是添加新的记录，这样的话这个回退也是可以回退的。

TODO: 完成数据 version 写表，需要有 version id，牵扯多个表的情况是需要有个统一的 versionid 生成机制，当相同的时候就对回滚至这个 versionid 就行

需要删除的数据的所有值的数据 delete 范围删除 那么对应的就需要把整个范围的数据都存储起来并

还有 对于多个表的情况 sql 语句肯定是需要完成拆分功能 。这样才能处理多个表的情况。

## 可替换的 sql 解析库

[sqlparser](https://github.com/marianogappa/sqlparser) 但是只能处理单表
[parser](https://github.com/pingcap/parser) 这个好

# 方案修改 1

> 参考: [postgres 的 CDC 方案](https://www.infoq.cn/article/lp5ucrkti3v4aw1pwvxm) > [pglogrepl](https://github.com/jackc/pglogrepl) 看了例子很好使用，能够解析 wal 日志并输出

1、要支持操作日志的查看
2、整体版本恢复的功能
3、简单修改表中字段

反向操作难度过高，而且在数据库中添加 callback 会导致数据执行效率降低

可以使用主从数据库的方式，监听主数据库的逻辑日志，与策略进行结合生成从数据库的表结构，并且从数据库只会保留之前一个月的日期时间的数据版本，过期自动清除，订阅到操作日志后加入到版本库中。

当需要查询的时候根据需要生成新的临时数据表，然后对日志进行 redolog

1、找到监听 postgres 的 redolog 方法(为之后写到 version 数据库做准备)

- ETL（Extract Transform Load）着眼于状态本身，用定时批量轮询的方式拉取状态本身。
- CDC（Changing Data Capture）则着眼于变更事件，以流式的方式持续收集状态变化事件（变更）

想在传统关系型数据库上实施 CDC 并不容易，关系型数据库本身的预写式日志 WAL 实际上就是数据库中变更事件的记录。因此从数据库中捕获变更，基本上可以认为等价于消费数据库产生的 WAL 日志/复制日志。（当然也有其他的变更捕获方式，例如在表上建立触发器，当变更发生时将变更记录写入另一张变更日志表，客户端不断 tail 这张日志表，当然也有一定的局限性）。

PostgreSQL 在实现逻辑复制的同时，已经提供了一切 CDC 所需要的基础设施：

- 逻辑解码（Logical Decoding），用于从 WAL 日志中解析逻辑变更事件：wal2json 插件(解析成 json 格式，但是是服务中的另外的插件，不考虑)、postgres10 的内置协议 pgoutput
- 复制协议（Replication Protocol）：提供了消费者实时订阅（甚至同步订阅）数据库变更的机制
- 快照导出（export snapshot）：允许导出数据库的一致性快照（pg_export_snapshot）
- 复制槽（Replication Slot），用于保存消费者偏移量，跟踪订阅者进度。

在 PostgreSQL 上实施 CDC 最为直观优雅的方式，就是按照 PostgreSQL 的复制协议编写一个"逻辑从库" ，从数据库中实时地，流式地接受逻辑解码后的变更事件，完成自己定义的处理逻辑，并及时向数据库汇报自己的消息消费进度。

逻辑解码：WAL 日志里包含了完整权威的变更事件记录，但这种记录格式过于底层。用户并不会对磁盘上某个数据页里的二进制变更（文件 A 页面 B 偏移量 C 追加写入二进制数据 D）感兴趣，他们感兴趣的是某张表中增删改了哪些行哪些字段。逻辑解码就是将物理变更记录翻译为用户期望的逻辑变更事件的机制（例如表 A 上的增删改事件）。

官方自带的 CDC 客户端样例——pg_recvlogical

2、postgres->sqlite3 数据转换器, 作为数据初始化，从数据库的选型，依赖 sqlite 与 postgres 的相似性，将需要处理的数据转换过来作为基础数据
3、设计策略表、version 表的模型(可以用上面方案的)
4、编写 redolog 处理器，结合数据策略，生成对应的 redolog 日志(这个简化的 redolog 只是为了便于当某个用户数据修改错误之后只能看某些字段，然后对应到某个 redolog 版本上后查看数据，然后对这些数据进行修改恢复)

# 方案修改 2

只需要记录核心的数据表以及字段，当发生 update、delete、insert 的操作的时候记录一下操作情况。

然后提供一个公共接口用于修改表的字段，某些行的值，添加数据之类的操作

1、触发器添加的函数编写，传入一个数据表，为数据表上添加对应情况的触发器，

```SQL
-- 触发器：在一条  INSERT, UPDATE, DELETE 语句执行前，后的函数。（自己定义）。
-- PostgreSQL语法：

-- 函数触发器
CREATE TRIGGER name { BEFORE | AFTER } { event [ OR ... ] } ON table [ FOR [ EACH ] { ROW | STATEMENT } ] EXECUTE PROCEDURE funcname ( arguments )
函数触发器:
CREATE OR REPLACE function del_xuesheng() RETURNS TRIGGER AS $DELETE$

BEGIN

DELETE FROM XUE_SHENG WHERE B_ID = OLD.id;

RETURN OLD;

END;
 $DELETE$


--- [修改触发器]: 如果函数返回空则不会执行触发器函数
create or replace function before_update() returns trigger as $$
declare
begin
  EXECUTE format('INSERT INTO %I.%I ("name", "age", "address", "salary", "action") VALUES (%s, %s, %s, %s, %s)'
                , TG_TABLE_SCHEMA, TG_TABLE_NAME || '_1', OLD.name, OLD.age, old.address, OLD.salary, 'before_update')
  using old;
  return old;
end
$$ language plpgsql;

create or replace function after_update() returns trigger as $$
declare
begin
    EXECUTE format('INSERT INTO %I.%I ("name", "age", "address", "salary", "action") VALUES (%s, %s, %s, %s, %s)'
                , TG_TABLE_SCHEMA, TG_TABLE_NAME || '_1', NEW.name, NEW.age, NEW.address, NEW.salary, 'after_update')
    using new;
    return new;
end
$$ language plpgsql;

create trigger companybeforeupdate before update on company for each row execute procedure before_update();
create trigger companyafterupdate after update on company for each row execute procedure after_update();

--- [insert触发器]
create or replace function before_insert() returns trigger as $$
declare
begin

end
$$ language plpgsql;

create or replace function after_insert() returns trigger as $$
declare
begin

end
$$ language plpgsql;
create trigger companybeforeupdate before insert on company for each row execute procedure before_insert();
create trigger companyafterupdate after insert on company for each row execute procedure after_insert();

--- [delete触发器]
create or replace function before_delete() returns trigger as $$
declare
begin

end
$$ language plpgsql;

create or replace function after_delete() returns trigger as $$
declare
begin

end
$$ language plpgsql;
create trigger companybeforeupdate before delete on company for each row execute procedure before_delete();
create trigger companyafterupdate after delete on company for each row execute procedure after_delete();
```

2、生成临时数据表，比如更新前获取字段值，更新后获取字段值，添加到备份表中。然后后台某个线程池对备份数据表请求获取值后删除掉，并将操作日志记录根据缓存策略来解析操作出来后存放到版本库中
