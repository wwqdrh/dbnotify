package handler

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ohko/hst"
)

// 校验请求是否为POST
func checkMethodPost(ctx *hst.Context) {
	if ctx.R.Method != "POST" {
		ctx.JSON2(http.StatusMethodNotAllowed, 0, map[string]interface{}{
			"err": "wrong method, use POST instead!",
		})
	}
}

func checkMethodGet(ctx *hst.Context) {
	if ctx.R.Method != "GET" {
		ctx.JSON2(http.StatusMethodNotAllowed, 0, map[string]interface{}{
			"err": "wrong method, use GET instead!",
		})
	}
}

// 解析json格式请求数据
func parseJSONBody(body io.ReadCloser, res interface{}) error {
	e := json.NewDecoder(body).Decode(res)
	if e != nil && e != io.EOF {
		return e
	}
	return nil
}
