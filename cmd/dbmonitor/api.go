package main

import "github.com/gin-gonic/gin"

func InitRouter(engine *gin.Engine) {
	engine.GET("/health", func(ctx *gin.Context) {
		ctx.String(200, "ok")
	})
	engine.GET("/register", Register)
	engine.GET("/unregister", UnRegister)
	engine.GET("/search", Search)
}

func Register(ctx *gin.Context) {
	table := ctx.Query("table")
	if table == "" {
		ctx.String(200, "请传入table")
		return
	}

	if err := dialet.Register(table); err != nil {
		ctx.String(200, err.Error())
	} else {
		ctx.String(200, "注册成功")
	}
}

func UnRegister(ctx *gin.Context) {
	table := ctx.Query("table")
	if table == "" {
		ctx.String(200, "请传入table")
		return
	}

	if err := dialet.UnRegister(table); err != nil {
		ctx.String(200, err.Error())
	} else {
		ctx.String(200, "取消成功")
	}
}

type SearchReq struct {
	Key   string `json:"key" form:"key"`
	Value string `json:"value" form:"value"`
	Table string `json:"table" form:"table"`
}

func Search(ctx *gin.Context) {
	var r SearchReq
	if err := ctx.ShouldBindQuery(&r); err != nil {
		ctx.String(400, "请传入key、value以及table")
		return
	}

	if sqlite3transport == nil {
		ctx.String(500, "未初始化完成，稍后重试")
		return
	}
	if data, err := sqlite3transport.Search(r.Table, r.Key, r.Value); err != nil {
		ctx.String(200, err.Error())
	} else {
		ctx.JSON(200, data)
	}
}
