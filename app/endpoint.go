package app

import (
	"net/http"
	"time"

	assetfs "github.com/elazarl/go-bindata-assetfs"
	"github.com/wwqdrh/datamanager/app/dto"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/base"
	"github.com/wwqdrh/datamanager/runtime"
)

var (
	// 静态资源
	fs = &assetfs.AssetFS{
		Asset: func(name string) ([]byte, error) {
			return Asset(name)
		},
		AssetDir: func(name string) ([]string, error) {
			return AssetDir(name)
		},
	}
)

type (
	HTTPHandler interface {
		Context(val interface{}) (*http.Request, http.ResponseWriter, error) // 获取request与response
		Get(string, func(interface{}))                                       // 用于挂载路由
		Post(string, func(interface{}))
		Static(string, http.FileSystem) // 挂载静态资源目录
	}
)

func Register(handler HTTPHandler) error {
	handler.Static("/bdlog/", fs)

	{
		l := Bdatalog{httpHandler: handler}
		handler.Get("/bdatalog/list_table", l.ListTable)
		handler.Post("/bdatalog/resigter_table", l.RegisterTable)
		handler.Post("/bdatalog/unregister_table", l.UnregisterTable)
		handler.Post("/bdatalog/list_history_by_name", l.ListHistoryByName)
		handler.Post("/bdatalog/list_history_all", l.ListHistoryAll)
		handler.Get("/bdatalog/list_table_field", l.ListTableField)
		handler.Post("/bdatalog/modify_table_policy", l.ModifyTablePolicy)
	}

	return nil
}

////////////////////
// endpoint
////////////////////

type Bdatalog struct {
	httpHandler HTTPHandler
}

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
func (bdl *Bdatalog) ListTable(ctx interface{}) {
	_, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	data := runtime.Runtime.GetWatcher().GetAllPolicy()
	dto.JSON2(w, 200, 0, data)
}

func (bdl *Bdatalog) RegisterTable(ctx interface{}) {
	r, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	var request dto.RegisterTableReq
	err = dto.ParseJSONBody(r.Body, &request)
	if err != nil || request.TableName == "" {
		dto.JSON2(w, 400, 1, "参数错误")
		return
	}

	if err := runtime.Runtime.GetWatcher().Register(&base.TablePolicy{
		Table:        request.TableName,
		MinLogNum:    request.MinLogNum,
		Outdate:      request.Outdate,
		SenseFields:  request.SenseFields,
		IgnoreFields: request.IgnoreFields}); err != nil {
		dto.JSON2(w, 500, 1, "参数错误")
	} else {
		dto.JSON2(w, 200, 1, "注册成功")
	}
}

func (bdl *Bdatalog) UnregisterTable(ctx interface{}) {
	r, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	var request dto.UnRegisterTableReq
	err = dto.ParseJSONBody(r.Body, &request)
	if err != nil || request.TableName == "" {
		dto.JSON2(w, 400, 1, "参数错误")
		return
	}

	if err = runtime.Runtime.GetWatcher().UnRegister(request.TableName); err != nil {
		dto.JSON2(w, 500, 1, "取消监听失败")
	} else {
		dto.JSON2(w, 200, 1, "取消监听失败")

	}
}

// {ListTableField}
// @Summary 数据表
// @Description 获取该系统记录表的所有字段
// @Tags 数据表
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} ListTableResponse "ok"
func (bdl *Bdatalog) ListTableField(ctx interface{}) {
	r, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	var request dto.ListTableFieldReq
	err = dto.ParseJSONBody(r.Body, &request)
	if err != nil || request.TableName == "" {
		dto.JSON2(w, 400, 1, "参数错误")
		return
	}

	dto.JSON2(w, 200, 0, runtime.Runtime.GetFieldHandler().GetFields(request.TableName))
}

type (
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
func (bdl *Bdatalog) ListHistoryByName(ctx interface{}) {
	r, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	var request dto.ListHistoryByNameReq
	err = dto.ParseJSONBody(r.Body, &request)
	if err != nil {
		dto.JSON2(w, 400, 1, "参数错误")
		return
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

	if !runtime.Runtime.GetWatcher().IsRegister(request.TableName) {
		w.WriteHeader(400)
		w.Write([]byte(request.TableName + "未进行监听"))
		return
	}

	logs, err := runtime.Runtime.GetLogSave().GetLogger(request.TableName, request.RecordID, start, end, request.Page, request.PageSize)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	}
	if err != nil {
		dto.JSON2(w, 500, 1, err.Error())

	} else {
		dto.JSON2(w, 200, 0, logs)

	}
}

func (bdl *Bdatalog) ListHistoryAll(ctx interface{}) {
	r, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	var request dto.ListHistoryAllReq
	err = dto.ParseJSONBody(r.Body, &request)
	if err != nil {
		dto.JSON2(w, 400, 1, "参数错误")
		return
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

	if !runtime.Runtime.GetWatcher().IsRegister(request.TableName) {
		w.WriteHeader(400)
		w.Write([]byte(request.TableName + "未进行监听"))
		return
	}

	logs, err := runtime.Runtime.GetLogSave().GetAllLog(request.TableName, runtime.Runtime.GetWatcher().GetSenseFields(request.TableName), start, end, request.Page, request.PageSize)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	}
	if err != nil {
		dto.JSON2(w, 500, 1, err.Error())

	} else {
		dto.JSON2(w, 200, 0, logs)

	}
}

// {name}
// @Summary
// @Description 修改表的策略配置接口，包括过期时间，监听字段，最少剩余记录条数
// @Tags
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} loginResponse "ok"
// @Failure 400 {object} spec.Failure "we need username"
// @Router /api/login [post]
func (bdl *Bdatalog) ModifyTablePolicy(ctx interface{}) {
	r, w, err := bdl.httpHandler.Context(ctx)
	if err != nil {
		dto.JSON2(w, 500, 1, "未知错误")
		return
	}

	var request dto.ModifyTablePolicyReq
	err = dto.ParseJSONBody(r.Body, &request)
	if err != nil {
		dto.JSON2(w, 400, 1, "参数错误")
		return
	}

	if err := runtime.Runtime.GetWatcher().ModifyPolicy(request.TableName, map[string]interface{}{
		"fields":      request.Fields,
		"out_date":    request.Outdate,
		"min_log_num": request.MinLogNum,
	}); err != nil {
		dto.JSON2(w, 500, 1, err.Error())

	} else {
		dto.JSON2(w, 200, 0, "修改成功")

	}
}
