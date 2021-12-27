package base

// 连接池管理

type IDriver interface {
	Ping() bool              // 连通性测试，
	Initail() error          // 连接状态初始化
	Connection() interface{} // 从连接池中获取管理器 本身存在连接池管理的可以忽略
	Close() error            // 关闭连接
}
