package base

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	sysModel "github.com/wwqdrh/datamanager/examples/main/system"

	"github.com/ohko/logger"
)

var (
	errNilHandle  = errors.New("服务句柄为空")
	errBaseHandle = errors.New("未派生的服务句柄")
)

// Service ...
type Service struct {
	sync.RWMutex

	wg      sync.WaitGroup
	name    string
	status  Status
	handle  IService
	mainlog *logger.Logger
	runners []IRunner
	stop    bool //需要停止
}

// Register 注册服务，并启动子业务go程
func (o *Service) Register() (err error) {
	o.SetStatus(RegistedStatus)
	defer func() {
		if err != nil {
			o.SetStatus(StopedStatus)
		}
	}()
	if o.handle == nil {
		err = errNilHandle
		return err
	}
	if _, ok := o.handle.(*Service); ok {
		o.SetStatus(StopedStatus)
		return err
	}

	baseLog.Log4Trace("Service: ", o.Name(), "starting ......")
	o.SetStatus(RunningStatus)
	// 启动子程
	runners := o.handle.Runners()
	for _, runner := range runners {
		o.SafeRun(runner)
	}
	baseLog.Log4Trace("Service: ", o.Name(), "started")

	go func() {
		// 等待所有子程结束
		o.wg.Wait()
		o.SetStatus(StopedStatus)
		baseLog.Log4Trace("Service: ", o.Name(), "stoped")
	}()
	return nil
}

// Runners ...
func (o *Service) Runners() []IRunner {
	return o.runners
}

// StatusCode ...
func (o *Service) StatusCode() Status {
	return o.status
}

// Status ...
func (o *Service) Status() string {
	return o.status.String()
}

// SetStatus ...
func (o *Service) SetStatus(st Status) {
	o.Lock()
	defer o.Unlock()
	o.status = st
	if st != StopedStatus {
		o.stop = false
	}
}

// Name ...
func (o *Service) Name() string {
	return o.name
}

// GetBase ...
func (o *Service) GetBase() *Service {
	return o
}

// GetMainLog ...
func (o *Service) GetMainLog() *logger.Logger {
	return o.mainlog
}

// getLog 获取日志，禁止外部调用
func (o *Service) getLog(ID string) *logger.Logger {
	f := logger.NewDefaultWriter(&logger.DefaultWriterOption{
		Clone:         os.Stdout,
		Path:          "./data/backend_log/",
		Label:         o.name,
		Name:          "log_",
		CompressMode:  logger.ModeDay,
		CompressCount: 2,
		CompressKeep:  30,
	})

	// goroutine id标识同一个业务逻辑流程
	goid := curGoroutineID()
	if goid != "" {
		ID = goid + "_" + ID
	}

	lg := logger.NewLogger(f)
	lg.SetPrefix(ID)

	return lg
}

// SafeRun ...
func (o *Service) SafeRun(runner IRunner, ts ...string) {
	tag := runner.Name()
	if len(ts) > 0 {
		tag = tag + "_" + ts[0]
	}

	o.wg.Add(1)
	go func() {
		baseLog.Log4Trace("  Runner: ", tag, "start running ......")
		defer func() {
			if err := recover(); err != nil {
				defer func() { recover() }()
				lg := o.GetMainLog()
				lg.LogCalldepth(5, logger.LoggerLevel4Trace, "  Runner: ", tag, "exit by ", err)
				dep := 0
				t := make([]string, 0, 10)
				for i := 1; i < 10; i++ {
					_, file, line, ok := runtime.Caller(i)
					if !ok {
						break
					}
					if strings.Contains(file, "/runtime/") || strings.Contains(file, "/reflect/") {
						continue
					}
					t = append(t, fmt.Sprintf("%s∟%s:%d", strings.Repeat(" ", dep), file, line))
					dep++
				}
				baseLog.Log4Trace(strings.Join(t, "\n"))
			}
			runner.SetStatus(StopedStatus)
			o.wg.Done()
		}()
		runner.SetLogger(func() *logger.Logger { return o.getLog(tag) })
		runner.SetStatus(RunningStatus)
		runner.Run()
	}()
}

// AssertStoped ...
func (o *Service) AssertStoped() {
	o.RLock()
	defer o.RUnlock()

	if o.stop {
		panic("stoping")
	}
}

// Sleep ...
func (o *Service) Sleep(d time.Duration) {
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

// Stop ...
func (o *Service) Stop() {
	func() {
		o.Lock()
		defer o.Unlock()
		if o.status != RunningStatus {
			return
		}
	}()
	o.stop = true
	o.wg.Wait()
	o.stop = false
}

// Debug ...
func (o *Service) Debug(lg *logger.Logger, v ...interface{}) {
	if lg == nil {
		lg = o.GetMainLog()
	}
	lg.LogCalldepth(3, logger.LoggerLevel0Debug, strings.ReplaceAll(fmt.Sprintln(v...), "\n", " "))
}

// Warn ...
func (o *Service) Warn(lg *logger.Logger, v ...interface{}) {
	if lg == nil {
		lg = o.GetMainLog()
	}
	lg.LogCalldepth(3, logger.LoggerLevel1Warning, strings.ReplaceAll(fmt.Sprintln(v...), "\n", " "))
}

// Error ...
func (o *Service) Error(lg *logger.Logger, v ...interface{}) {
	if lg == nil {
		lg = o.GetMainLog()
	}
	lg.LogCalldepth(3, logger.LoggerLevel2Error, strings.ReplaceAll(fmt.Sprintln(v...), "\n", " "))
}

// LogDB 重要日志记录到数据库中
func (o *Service) LogDB(runner, title, key string, success bool, reason ...string) {
	lg := &sysModel.ServiceLog{
		Service: o.Name(),
		Runner:  runner,
		Title:   title,
		Key:     key,
		Success: success,
	}
	if len(reason) > 0 {
		lg.Reason = reason[0]
	}
	sysModel.DBServiceLog.Create(lg)
}

// AppendRunner ...
func (o *Service) AppendRunner(runner IRunner, tag string) {
	o.Lock()
	defer o.Unlock()

	runner.SetService(o.handle)
	runner.SetName(fmt.Sprintf("%v-%d", getName(runner), len(o.runners)))
	o.runners = append(o.runners, runner)

	o.SafeRun(runner, tag)
}

var goroutineSpace = []byte("goroutine ")

func curGoroutineID() string {
	bp := littleBuf.Get().(*[]byte)
	defer littleBuf.Put(bp)
	b := *bp
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, goroutineSpace)
	i := bytes.IndexByte(b, ' ')
	if i < 0 {
		return ""
	}
	b = b[:i]
	return string(b)
}

var littleBuf = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 64)
		return &buf
	},
}
