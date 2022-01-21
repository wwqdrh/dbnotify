package base

import (
	"github.com/ohko/logger"
)

// ...
const (
	UnregistedStatus Status = iota
	RegistedStatus
	RunningStatus
	StopedStatus
)

var (
	services []IService
)

// IService 后台服务模型接口
type IService interface {
	Register() error    // 注册
	StatusCode() Status // 状态
	Status() string     // 状态描述
	Name() string       // 名称
	Runners() []IRunner // 获取子业务go程
	GetBase() *Service
	Stop()
}

// IRunner 一个运行业务的go程接口
type IRunner interface {
	SetStatus(Status)
	Status() string
	StatusCode() Status
	Stop()
	SetLogger(func() *logger.Logger)
	SetLogLevel(level int)
	GetLogLevel() int
	SetService(IService)
	SetName(string)
	Name() string
	Run()
	GetService() IService
}

// Status ...
type Status int

// String ...
func (st Status) String() string {
	switch st {
	default:
		return "UnknowStatus"
	case UnregistedStatus:
		return "UnregistedStatus"
	case RegistedStatus:
		return "RegistedStatus"
	case RunningStatus:
		return "RunningStatus"
	case StopedStatus:
		return "StopedStatus"
	}
}

// Services ...
func Services() []IService {
	return services
}

// AppendService ...
func AppendService(s IService) {
	services = append(services, s)
}
