package app

import (
	"net/http"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/ohko/hst"
	"github.com/wwqdrh/datamanager/app/handler"
)

type HTTPHandler interface {
	Context(val interface{}) (*http.Request, http.ResponseWriter, error) // 获取request与response
	Get(string, func(interface{}))                                       // 用于挂载路由
}

func Register(httpHandler HTTPHandler) error {
	// app.RegisterApi(httpHandler)
	return nil
}

// 启动静态服务
func FileServer(h *hst.HST) {
	fs := &assetfs.AssetFS{
		Asset: func(name string) ([]byte, error) {
			return Asset(name)
		},
		AssetDir: func(name string) ([]string, error) {
			return AssetDir(name)
		},
	}

	h.Handle("/bdlog/", http.FileServer(fs))
}

func RegisterApi(h *hst.HST, middlewares ...hst.HandlerFunc) *hst.HST {
	if h == nil {
		h = hst.New(nil)
	}

	ms := append([]hst.HandlerFunc{}, middlewares...)

	h.RegisterHandle(ms,
		&handler.Bdatalog{},
	)

	FileServer(h)

	return h
}
