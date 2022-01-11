package common

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

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

type (
	BaseResponse struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data"`
	}
	Bdatalog struct{}
)

// 操作主要是针对单个数据表的情况
// 1、列出有哪些表，去看本地的策略表中存储的东西
// 2、查看表的历史记录，结合本地策略的东西
// 3、修改策略
// 4、表的修改

type (
	ListTableRequest  struct{}
	ListTableResponse struct {
		Code int
		Data interface{}
	}
)

// {ListTable}
// @Summary 数据表
// @Description 获取该系统中监控的所有记录表
// @Tags 数据表
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} ListTableResponse "ok"
// @Router /table/list [get]
func (bdl *Bdatalog) ListTable(c *hst.Context) {
	checkMethodGet(c)

	c.JSON2(http.StatusOK, 0, Mana.ListTable2())
}

type (
	ListTableFieldRequest struct {
		TableName string `json:"table_name" binding:"required"`
	}
)

// {ListTableField}
// @Summary 数据表
// @Description 获取该系统记录表的所有字段
// @Tags 数据表
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} ListTableResponse "ok"
// @Router /table/list [get]
func (bdl *Bdatalog) ListTableField(c *hst.Context) {
	checkMethodGet(c)

	var request ListTableFieldRequest
	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}

	c.JSON2(http.StatusOK, 0, Mana.ListTableField(request.TableName))
}

type (
	ListHistoryByNameRequest struct {
		TableName string     `form:"table_name" json:"table_name" binding:"required"`
		StartTime *time.Time `form:"start_time"`
		EndTime   *time.Time `form:"end_time"`
		Page      int        `json:"page"`
		PageSize  int        `json:"page_size"`
	}
	ListHistoryByNameResponse struct {
		Log    map[string]interface{} `json:"log"`
		Time   time.Time              `json:"time"`
		Action string                 `json:"action"`
	}
)

// {ListHistoryByName}
// @Summary 数据表
// @Description 根据数据表名以及时间范围进行获取历史记录
// @Tags  数据表
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} loginResponse "ok"
// @Router /table/history [get]
func (bdl *Bdatalog) ListHistoryByName(c *hst.Context) {
	// 去对应表的log中读取并返回整个json格式可以稍作处理
	checkMethodGet(c)
	var (
		request ListHistoryByNameRequest
		err     error
	)

	err = parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}

	logs, err := Mana.ListTableLog(request.TableName, request.StartTime, request.EndTime, request.Page, request.PageSize)
	if err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	}

	c.JSON2(http.StatusOK, 0, logs)
}

type (
	ModifyTablePolicyRequest struct {
		TableName    string   `json:"table_name" binding:"required"`
		ModifyFields []string `json:"modify_fields" binding:"required"`
		MinLogNum    int      `json:"min_log_num"`
		Fields       []string `json:"fields"`
		Outdate      int      `json:"out_date"`
	}
)

// {name}
// @Summary
// @Description 修改表的策略配置接口，包括过期时间，监听字段，最少剩余记录条数
// @Tags
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} loginResponse "ok"
// @Failure 400 {object} spec.Failure "we need username"
// @Router /api/login [post]
func (bdl *Bdatalog) ModifyTablePolicy(c *hst.Context) {
	var (
		request ModifyTablePolicyRequest
	)
	if err := parseJSONBody(c.R.Body, &request); err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	}

	modifyFields := map[string]interface{}{}
	for _, item := range request.ModifyFields {
		modifyFields[strings.ToLower(item)] = true
		field := strings.ToLower(item)
		if field == "fields" {
			modifyFields[field] = request.Fields
		} else if field == "out_date" {
			modifyFields[field] = request.Outdate
		} else if field == "min_log_num" {
			modifyFields[field] = request.MinLogNum
		}
	}
	if err := Mana.ModifyPolicy(request.TableName, modifyFields); err != nil {
		c.JSON2(http.StatusBadRequest, 2, err.Error())
	}

	c.JSON2(http.StatusOK, 0, "修改成功")
}
