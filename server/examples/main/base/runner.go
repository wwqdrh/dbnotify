package base

import (
	"fmt"
	"strings"
	"time"

	"github.com/ohko/logger"
)

// Runner 服务的子业务go程
type Runner struct {
	service IService
	name    string
	status  Status
	stop    bool
	lg      *logger.Logger
	logLeve int
}

// SetStatus ...
func (o *Runner) SetStatus(st Status) {
	o.status = st
	if st != StopedStatus {
		o.stop = false
	}
}

// Status ...
func (o *Runner) Status() string {
	return o.status.String()
}

// StatusCode ...
func (o *Runner) StatusCode() Status {
	return o.status
}

// SetLogger ...
func (o *Runner) SetLogger(f func() *logger.Logger) {
	if o.lg == nil {
		o.lg = f()
	}
}

func (o *Runner) SetLogLevel(level int) {
	if o.lg != nil {
		o.lg.SetLevel(level)
		o.logLeve = level
	}
}

func (o *Runner) GetLogLevel() int {
	return o.logLeve
}

// SetService ...
func (o *Runner) SetService(h IService) {
	o.service = h
}

// Run ...
func (o *Runner) Run() {
	return
}

// AssertStoped 如果服务发出了停止请求
func (o *Runner) AssertStoped() {
	o.service.GetBase().AssertStoped()
	if o.stop {
		o.status = StopedStatus
		panic("stoping")
	}
}

func (o *Runner) Stop() {
	o.stop = true
	for i := 0; i < 3; i++ {
		time.Sleep(time.Millisecond * 10)
		if o.status == StopedStatus {
			break
		}
	}
}

// Sleep ...
func (o *Runner) Sleep(d time.Duration) {
	if d <= 0 {
		o.AssertStoped()
		return
	}

	timer := time.NewTimer(d)
	ticker := time.NewTicker(time.Millisecond * 100)

	defer func() {
		timer.Stop()
		ticker.Stop()
		o.AssertStoped()
	}()

	for {
		select {
		case <-timer.C:
			return
		case <-ticker.C:
			o.AssertStoped()
		}
	}
}

// Debug ...
func (o *Runner) Debug(v ...interface{}) {
	if o.lg == nil {
		return
	}
	o.lg.LogCalldepth(3, logger.LoggerLevel0Debug, strings.ReplaceAll(fmt.Sprintln(v...), "\n", " "))
}

// Warn ...
func (o *Runner) Warn(v ...interface{}) {
	if o.lg == nil {
		return
	}
	o.lg.LogCalldepth(3, logger.LoggerLevel1Warning, strings.ReplaceAll(fmt.Sprintln(v...), "\n", " "))
}

// Error ...
func (o *Runner) Error(v ...interface{}) {
	if o.lg == nil {
		return
	}
	o.lg.LogCalldepth(3, logger.LoggerLevel2Error, strings.ReplaceAll(fmt.Sprintln(v...), "\n", " "))
}

// LogDB 重要日志记录到数据库中
func (o *Runner) LogDB(title, key string, success bool, reason ...string) {
	o.service.GetBase().LogDB(o.name, title, key, success, reason...)
}

// SetName ...
func (o *Runner) SetName(name string) {
	o.name = name
}

// Name ...
func (o *Runner) Name() string {
	return o.service.GetBase().Name() + "_" + o.name
}

// GetService ...
func (o *Runner) GetService() IService {
	return o.service
}
