package datamanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/wwqdrh/datamanager/dialet"
)

// 提供基于http的远程调用，用户能够进行注册
// 当程序检测到自定义表的更新时自动调用http接口进行通知

func GetEvent() chan *Event {
	return make(chan *Event, 10)
}

type Event struct {
	Schema  string                 // example: public.table
	Op      int                    // 操作类型 增、删、改、查
	Payload map[string]interface{} // 当前数据信息
}

type Watcher struct {
	dial dialet.IDialet
	cb   map[string]string
}

func NewWatcher(dial dialet.IDialet) *Watcher {
	return &Watcher{
		dial: dial,
		cb:   map[string]string{},
	}
}

// now just a table event
// a get url callback
func (w *Watcher) Register(table string, url string) {
	w.cb[table] = url
}

func (w *Watcher) Notify(ctx context.Context) {
	eventChan := w.dial.Watch(ctx)
	for {
		select {
		case e := <-eventChan:
			if val, ok := e.(dialet.ILogData); !ok {
				fmt.Println("数据错误")
			} else if url, ok := w.cb[val.GetTable()]; ok {
				err := w.HTTPPost(url, map[string]interface{}{
					"table":   val.GetTable(),
					"payload": val.GetPaylod(),
				})
				if err != nil {
					fmt.Println(err)
				}
			} else {
				fmt.Println("未注册")
			}
		case <-ctx.Done():
			return
		}
	}
}

// send data to url, the method is post
func (w *Watcher) HTTPPost(url string, data interface{}) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Close = true
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
