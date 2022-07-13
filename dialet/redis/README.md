## 命令产生通知的类型

DEL 命令为每个被删除的键产生一个 del 通知。
RENAME 产生两个通知：为来源键（source key）产生一个 rename_from 通知，并为目标键（destination key）产生一个 rename_to 通知。
EXPIRE 和 EXPIREAT 在键被正确设置过期时间时产生一个 expire 通知。当 EXPIREAT 设置的时间已经过期，或者 EXPIRE 传入的时间为负数值时，键被删除，并产生一个 del 通知。
SORT 在命令带有 STORE 参数时产生一个 sortstore 事件。如果 STORE 指示的用于保存排序结果的键已经存在，那么程序还会发送一个 del 事件。
SET 以及它的所有变种（SETEX 、 SETNX 和 GETSET）都产生 set 通知。其中 SETEX 还会产生 expire 通知。
MSET 为每个键产生一个 set 通知。
SETRANGE 产生一个 setrange 通知。
INCR 、 DECR 、 INCRBY 和 DECRBY 都产生 incrby 通知。
INCRBYFLOAT 产生 incrbyfloat 通知。
APPEND 产生 append 通知。
LPUSH 和 LPUSHX 都产生单个 lpush 通知，即使有多个输入元素时，也是如此。
RPUSH 和 RPUSHX 都产生单个 rpush 通知，即使有多个输入元素时，也是如此。
RPOP 产生 rpop 通知。如果被弹出的元素是列表的最后一个元素，那么还会产生一个 del 通知。
LPOP 产生 lpop 通知。如果被弹出的元素是列表的最后一个元素，那么还会产生一个 del 通知。
LINSERT 产生一个 linsert 通知。
LSET 产生一个 lset 通知。
LTRIM 产生一个 ltrim 通知。如果 LTRIM 执行之后，列表键被清空，那么还会产生一个 del 通知。
RPOPLPUSH 和 BRPOPLPUSH 产生一个 rpop 通知，以及一个 lpush 通知。两个命令都会保证 rpop 的通知在 lpush 的通知之前分发。如果从键弹出元素之后，被弹出的列表键被清空，那么还会产生一个 del 通知。
HSET 、 HSETNX 和 HMSET 都只产生一个 hset 通知。
HINCRBY 产生一个 hincrby 通知。
HINCRBYFLOAT 产生一个 hincrbyfloat 通知。
HDEL 产生一个 hdel 通知。如果执行 HDEL 之后，哈希键被清空，那么还会产生一个 del 通知。
SADD 产生一个 sadd 通知，即使有多个输入元素时，也是如此。
SREM 产生一个 srem 通知，如果执行 SREM 之后，集合键被清空，那么还会产生一个 del 通知。
SMOVE 为来源键（source key）产生一个 srem 通知，并为目标键（destination key）产生一个 sadd 事件。
SPOP 产生一个 spop 事件。如果执行 SPOP 之后，集合键被清空，那么还会产生一个 del 通知。
SINTERSTORE 、 SUNIONSTORE 和 SDIFFSTORE 分别产生 sinterstore 、 sunionostore 和 sdiffstore 三种通知。如果用于保存结果的键已经存在，那么还会产生一个 del 通知。
ZINCRBY 产生一个 zincr 通知。（译注：非对称，请注意。）
ZADD 产生一个 zadd 通知，即使有多个输入元素时，也是如此。
ZREM 产生一个 zrem 通知，即使有多个输入元素时，也是如此。如果执行 ZREM 之后，有序集合键被清空，那么还会产生一个 del 通知。
ZREMRANGEBYSCORE 产生一个 zrembyscore 通知。（译注：非对称，请注意。）如果用于保存结果的键已经存在，那么还会产生一个 del 通知。
ZREMRANGEBYRANK 产生一个 zrembyrank 通知。（译注：非对称，请注意。）如果用于保存结果的键已经存在，那么还会产生一个 del 通知。
ZINTERSTORE 和 ZUNIONSTORE 分别产生 zinterstore 和 zunionstore 两种通知。如果用于保存结果的键已经存在，那么还会产生一个 del 通知。
每当一个键因为过期而被删除时，产生一个 expired 通知。
每当一个键因为 maxmemory 政策而被删除以回收内存时，产生一个 evicted 通知。

Redis 使用以下两种方式删除过期的键：

当一个键被访问时，程序会对这个键进行检查，如果键已经过期，那么该键将被删除。
底层系统会在后台渐进地查找并删除那些过期的键，从而处理那些已经过期、但是不会被访问到的键。
当过期键被以上两个程序的任意一个发现、 并且将键从数据库中删除时， Redis 会产生一个 expired 通知。

Redis 并不保证生存时间（TTL）变为 0 的键会立即被删除： 如果程序没有访问这个过期键， 或者带有生存时间的键非常多的话， 那么在键的生存时间变为 0 ， 直到键真正被删除这中间， 可能会有一段比较显著的时间间隔。

因此， Redis 产生 expired 通知的时间为过期键被删除的时候， 而不是键的生存时间变为 0 的时候。