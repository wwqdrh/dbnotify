package request

// TablePolicy 用于初始化时传入
type TablePolicy struct {
	Table        interface{}
	RelaField    string
	Relations    string
	SenseFields  []string
	IgnoreFields []string
}
