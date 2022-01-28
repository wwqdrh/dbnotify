package router

// 就不先写router 然后 api了 这里简化直接用hst比较好接入

import (
	"datamanager/server/service"
	"encoding/json"
	"io"
	"net/http"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/ohko/hst"
)

var dblogService = service.ServiceGroupApp.Dblog

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

	c.JSON2(http.StatusOK, 0, dblogService.Meta.ListTable2())
}

func (bdl *Bdatalog) RegisterTable(c *hst.Context) {
	checkMethodPost(c)

	var request struct {
		TableName   string   `json:"table_name" binding:"required"`
		SenseFields []string `json:"sense_fields" binding:"required"`
		MinLogNum   int      `json:"min_log_num" binding:"required"`
		Outdate     int      `json:"out_date" binding:"required"`
	}

	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}
	if request.TableName == "" {
		c.JSON2(http.StatusOK, -1, "table_name不能为空")
	}

	if err = dblogService.Meta.Register(request.TableName, request.MinLogNum, request.Outdate, request.SenseFields, nil); err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	} else {
		c.JSON2(http.StatusOK, 0, "注册成功")
	}

}

func (bdl *Bdatalog) UnregisterTable(c *hst.Context) {
	checkMethodPost(c)

	var request struct {
		TableName string `json:"table_name" binding:"required"`
	}

	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}
	if request.TableName == "" {
		c.JSON2(http.StatusOK, -1, "table_name不能为空")
	}
	if err = dblogService.Meta.UnRegister(request.TableName); err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	} else {
		c.JSON2(http.StatusOK, 0, "取消监听成功")
	}
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
func (bdl *Bdatalog) ListTableField(c *hst.Context) {
	checkMethodPost(c)

	var request ListTableFieldRequest
	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}

	c.JSON2(http.StatusOK, 0, dblogService.Meta.ListTableField(request.TableName))
}

type (
	ListHistoryByNameRequest struct {
		TableName string `form:"table_name" json:"table_name" binding:"required"`
		RecordID  string `form:"record_id" json:"record_id" binding:"required"`
		// StartTime *time.Time `form:"start_time" json:"start_time" time_format:"unix"`
		// EndTime   *time.Time `form:"end_time" json:"end_time" time_format:"unix"`
		StartTime int64 `json:"start_time"`
		EndTime   int64 `json:"end_time"`
		Page      int   `json:"page"`
		PageSize  int   `json:"page_size"`
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
	checkMethodPost(c)
	var (
		request ListHistoryByNameRequest
		err     error
	)

	err = parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}
	var start, end *time.Time
	if request.StartTime != 0 {
		t := time.Unix(request.StartTime/1000, 0)
		start = &t
	}
	if request.EndTime != 0 {
		t := time.Unix(request.EndTime/1000, 0)
		end = &t
	}
	logs, err := dblogService.Meta.ListTableLog(request.TableName, request.RecordID, start, end, request.Page, request.PageSize)
	if err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	}

	c.JSON2(http.StatusOK, 0, logs)
}

type (
	ListHistoryAllRequest struct {
		TableName string `form:"table_name" json:"table_name" binding:"required"`
		// StartTime *time.Time `form:"start_time" json:"start_time" time_format:"unix"`
		// EndTime   *time.Time `form:"end_time" json:"end_time" time_format:"unix"`
		StartTime int64 `json:"start_time"`
		EndTime   int64 `json:"end_time"`
		Page      int   `json:"page"`
		PageSize  int   `json:"page_size"`
	}
)

func (bdl *Bdatalog) ListHistoryAll(c *hst.Context) {
	checkMethodPost(c)
	var (
		request ListHistoryAllRequest
		err     error
	)

	err = parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}

	var start, end *time.Time
	if request.StartTime != 0 {
		t := time.Unix(request.StartTime/1000, 0)
		start = &t
	}
	if request.EndTime != 0 {
		t := time.Unix(request.EndTime/1000, 0)
		end = &t
	}

	logs, err := dblogService.Meta.ListTableAllLog(request.TableName, start, end, request.Page, request.PageSize)
	if err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	}

	c.JSON2(http.StatusOK, 0, logs)
}

type (
	ModifyTablePolicyRequest struct {
		TableName string   `json:"table_name" binding:"required"`
		MinLogNum int      `json:"min_log_num"`
		Fields    []string `json:"fields"`
		Outdate   int      `json:"out_date"`
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
	checkMethodPost(c)
	var (
		request ModifyTablePolicyRequest
	)
	if err := parseJSONBody(c.R.Body, &request); err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	}

	if err := dblogService.Meta.ModifyPolicy(request.TableName, map[string]interface{}{
		"fields":      request.Fields,
		"out_date":    request.Outdate,
		"min_log_num": request.MinLogNum,
	}); err != nil {
		c.JSON2(http.StatusBadRequest, 2, err.Error())
	}

	c.JSON2(http.StatusOK, 0, "修改成功")
}
