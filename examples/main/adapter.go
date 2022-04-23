package main

import (
	"errors"
	"net/http"

	"github.com/ohko/hst"
)

type hstAdapter struct {
	engine *hst.HST
}

func NewHSTAdapter(engine *hst.HST) *hstAdapter {
	return &hstAdapter{engine: engine}
}

func (g hstAdapter) Context(val interface{}) (*http.Request, http.ResponseWriter, error) {
	ctx, err := val.(*hst.Context)
	if !err {
		return nil, nil, errors.New("类型转换失败")
	}
	return ctx.R, ctx.W.ResponseWriter, nil
}

func (g hstAdapter) Static(url string, path http.FileSystem) {
	g.engine.Handle(url, http.FileServer(path))
}

func (g hstAdapter) Get(url string, fn func(interface{})) {
	g.engine.GET(url, func(ctx *hst.Context) {
		fn(ctx)
	})
}

func (g hstAdapter) Post(url string, fn func(interface{})) {
	g.engine.POST(url, func(ctx *hst.Context) {
		fn(ctx)
	})
}
