package base

import (
	"fmt"
	"os"
	"reflect"

	"github.com/ohko/logger"
)

var (
	baseLog = logger.NewLogger(logger.NewDefaultWriter(&logger.DefaultWriterOption{
		Clone:         os.Stdout,
		Path:          "./data/backend_log/",
		Label:         "base",
		Name:          "log_",
		CompressMode:  logger.ModeDay,
		CompressCount: 2,
		CompressKeep:  30,
	}))
)

// InitService 初始化一个服务，并添加到服务列表中
func InitService(h IService, runners ...IRunner) {
	if h == nil {
		return
	}
	o := h.GetBase()

	// 必须基于Service去派生
	if o == nil {
		return
	}

	o.handle = h
	o.name = getName(h)
	o.mainlog = o.getLog("main")
	for i, r := range runners {
		r.SetService(o.handle)
		r.SetName(fmt.Sprintf("%v-%d", getName(r), i))
	}
	o.runners = runners
	o.SetStatus(UnregistedStatus)

	AppendService(h)
}

func getName(i interface{}) string {
	t := reflect.TypeOf(i)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	return t.Name()
}
