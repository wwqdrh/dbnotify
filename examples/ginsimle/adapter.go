package main

import (
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/gin-gonic/gin"
)

type GinHanlderAdapter struct {
	engine *gin.Engine
}

func (g GinHanlderAdapter) Context(val interface{}) (*http.Request, http.ResponseWriter, error) {
	ctx, err := val.(*gin.Context)
	if !err {
		return nil, nil, errors.New("类型转换失败")
	}

	return ctx.Request, ctx.Writer, nil
}

func (g GinHanlderAdapter) Static(url string, filepath http.FileSystem) {
	f := http.FileServer(filepath)
	url = path.Join(url, "/*filepath")
	g.engine.HEAD(url, func(ctx *gin.Context) {
		ctx.Request.URL.Path = strings.TrimPrefix(ctx.Request.URL.Path, url)
		f.ServeHTTP(ctx.Writer, ctx.Request)
	})
	g.engine.GET(url, func(ctx *gin.Context) {
		ctx.Request.URL.Path = strings.TrimPrefix(ctx.Request.URL.Path, url)
		f.ServeHTTP(ctx.Writer, ctx.Request)
	})
}

func (g GinHanlderAdapter) Get(url string, fn func(interface{})) {
	g.engine.GET(url, func(ctx *gin.Context) {
		fn(ctx)
	})
}

func (g GinHanlderAdapter) Post(url string, fn func(interface{})) {
	g.engine.POST(url, func(ctx *gin.Context) {
		fn(ctx)
	})
}
