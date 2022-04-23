package base

type TablePolicy struct {
	Table        interface{}
	MinLogNum    int
	Outdate      int
	RelaField    string
	Relations    string
	SenseFields  []string
	IgnoreFields []string
}

type IWatcher interface {
	// 初始化
	Initail() error

	// 使用不同的策略进行注册
	Register(policy *TablePolicy)

	// 所有的表 包括动态创建的表
	ListenAll() chan interface{}

	// 以table粒度进行监听
	ListenTable(tableName string) chan interface{}
} // 不同的策略
