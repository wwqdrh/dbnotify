package fulltrigger

// 全量trigger策略
import (
	"fmt"
	"time"

	"github.com/wwqdrh/datamanager/core"
	"github.com/wwqdrh/datamanager/domain/stream/entity"
	stream_repo "github.com/wwqdrh/datamanager/domain/stream/repository"
	"github.com/wwqdrh/datamanager/internal/datautil"
)

var (
	defaultLogLoad  *DefaultLogLoad
	defaultLogStore []*DefaultLogStore
)

type (
	DefaultLogLoad struct {
		queue *datautil.Queue
		fn    func(tableName string, id uint64, num int) ([]*entity.LogTable, error)
	}

	DefaultLogStore struct {
		queue *datautil.Queue
		fn    func(policy *entity.Policy, data []*entity.LogTable) error
	}
)

// StartFullTrigger 全量触发器启动
func StartFullTrigger() {
	defaultLogLoad = newLogLoadRunner(core.G_LogTaskQueue, DblogService.Load())
	defaultLogStore = newLogStoreRunner(core.G_LogTaskQueue, 10, DblogService.Store())

	go defaultLogLoad.Run()
	for _, item := range defaultLogStore {
		go item.Run()
	}
}

// 生产者
func newLogLoadRunner(queue *datautil.Queue, fn func(tableName string, id uint64, num int) ([]*entity.LogTable, error)) *DefaultLogLoad {
	return &DefaultLogLoad{
		queue: queue,
		fn:    fn, // service.DblogService.Load()
	}
}

// 消费者
func newLogStoreRunner(queue *datautil.Queue, num int, fn func(policy *entity.Policy, data []*entity.LogTable) error) []*DefaultLogStore {
	rs := make([]*DefaultLogStore, num)

	for i := 0; i < num; i++ {
		rs[i] = &DefaultLogStore{
			queue: queue,
			fn:    fn, // service.DblogService.Store(),
		}
	}

	return rs
}

// 每隔一段时间读取n条数据加入到任务队列中
func (r *DefaultLogLoad) Run() {
	var id uint64

	for {
		MetaService.GetAllPolicy().Range(func(key, value interface{}) bool {
			tableName := key.(string)
			policy := value.(*entity.Policy)
			res, err := r.fn(tableName, id, core.G_CONFIG.DataLog.PerReadNum)
			if err != nil {
				fmt.Println("获取数据失败", err.Error())
				return true
			}
			if len(res) == 0 {
				return true
			}

			if !<-r.queue.Push(map[string]interface{}{
				"data":   res,
				"policy": policy,
			}) {
				return true
			}
			if err := stream_repo.LogTableRepo.DeleteByID(res); err != nil {
				fmt.Println("删除数据失败:", err.Error())
				return true
			}

			if res[len(res)-1].ID > id {
				id = res[len(res)-1].ID
			}
			return true
		})
		time.Sleep(time.Minute * 10)
	}
}

func (r *DefaultLogStore) Run() {
	for {
		if task := r.queue.GetTask(); task != nil {
			payload := task.PayLoad.(map[string]interface{}) // data, policy
			data := payload["data"].([]*entity.LogTable)
			policy := payload["policy"].(*entity.Policy)
			if err := r.fn(policy, data); err != nil {
				fmt.Println("消费者执行错误", err.Error())
				task.SetResult(false)
			} else {
				task.SetResult(true)
			}
		}

		time.Sleep(10 * time.Second)
	}
}
