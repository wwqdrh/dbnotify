package pgwatcher

type TablePolicy struct {
	Table        interface{}
	MinLogNum    int
	Outdate      int
	RelaField    string
	Relations    string
	SenseFields  []string
	IgnoreFields []string
}

type watcherPolicy int

const (
	table watcherPolicy = iota
	trigger
)

type IWatcher interface {
	// 使用不同的策略进行注册
	Register(policy *TablePolicy)

	// 所有的表 包括动态创建的表
	ListenAll() chan interface{}

	// 以table粒度进行监听
	ListenTable(tableName string) chan interface{}
} // 不同的策略

func NewWatcherPolicy(p watcherPolicy) IWatcher {
	switch p {
	case table:
		return nil
	case trigger:
		return nil
	default:
		return nil
	}
}
