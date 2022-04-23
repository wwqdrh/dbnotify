package main

import (
	"errors"
	"net/http"

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

func (g GinHanlderAdapter) Static(url string, path http.FileSystem) {
	g.engine.StaticFS(url, path)
}

func (g GinHanlderAdapter) Get(url string, fn func(interface{})) {
	g.engine.GET(url, func(ctx *gin.Context) {
		fn(ctx)
	})
}

func main() {
	engine := gin.Default()

	srv := http.Server{
		Handler: engine,
		Addr:    ":8080",
	}

	srv.ListenAndServe()
}
