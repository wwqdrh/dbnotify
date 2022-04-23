package handler

import (
	"net/http"
	"time"

	"github.com/wwqdrh/datamanager/app/dto"
	"github.com/wwqdrh/datamanager/internal/pgwatcher/base"
	"github.com/wwqdrh/datamanager/runtime"

	"github.com/ohko/hst"
)

var R = runtime.Runtime

type (
	Bdatalog struct{}
)

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

	c.JSON2(http.StatusOK, 0, runtime.Runtime.GetWatcher().GetAllPolicy())
}

func (bdl *Bdatalog) RegisterTable(c *hst.Context) {
	checkMethodPost(c)
	var request dto.RegisterTableReq
	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}
	if request.TableName == "" {
		c.JSON2(http.StatusOK, -1, "table_name不能为空")
	}

	if err := runtime.Runtime.GetWatcher().Register(&base.TablePolicy{
		Table:        request.TableName,
		MinLogNum:    request.MinLogNum,
		Outdate:      request.Outdate,
		SenseFields:  request.SenseFields,
		IgnoreFields: request.IgnoreFields}); err != nil {
		c.JSON2(http.StatusOK, 1, "注册失败")
	} else {
		c.JSON2(http.StatusOK, 0, "注册成功")
	}
}

func (bdl *Bdatalog) UnregisterTable(c *hst.Context) {
	checkMethodPost(c)
	var request dto.UnRegisterTableReq

	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}
	if request.TableName == "" {
		c.JSON2(http.StatusOK, -1, "table_name不能为空")
	}
	if err = runtime.Runtime.GetWatcher().UnRegister(request.TableName); err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	} else {
		c.JSON2(http.StatusOK, 0, "取消监听成功")
	}
}

// {ListTableField}
// @Summary 数据表
// @Description 获取该系统记录表的所有字段
// @Tags 数据表
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} ListTableResponse "ok"
func (bdl *Bdatalog) ListTableField(c *hst.Context) {
	checkMethodPost(c)

	var request dto.ListTableFieldReq
	err := parseJSONBody(c.R.Body, &request)
	if err != nil {
		c.JSON2(http.StatusOK, 1, err.Error())
	}

	c.JSON2(http.StatusOK, 0, runtime.Runtime.GetFieldHandler().GetFields(request.TableName))
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
func (bdl *Bdatalog) ListHistoryByName(c *hst.Context) {
	// 去对应表的log中读取并返回整个json格式可以稍作处理
	checkMethodPost(c)
	var (
		request dto.ListHistoryByNameReq
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

	if !R.GetWatcher().IsRegister(request.TableName) {
		c.JSON2(http.StatusOK, 1, request.TableName+"未进行监听")
		return
	}

	if logs, err := R.GetLogSave().GetLogger(request.TableName, request.RecordID, start, end, request.Page, request.PageSize); err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	} else {
		c.JSON2(http.StatusOK, 0, logs)
	}
}

func (bdl *Bdatalog) ListHistoryAll(c *hst.Context) {
	checkMethodPost(c)
	var (
		request dto.ListHistoryAllReq
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

	if !R.GetWatcher().IsRegister(request.TableName) {
		c.JSON2(http.StatusOK, 1, request.TableName+"未进行监听")
		return
	}

	if logs, err := R.GetLogSave().GetAllLog(request.TableName, R.GetWatcher().GetSenseFields(request.TableName), start, end, request.Page, request.PageSize); err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	} else {
		c.JSON2(http.StatusOK, 0, logs)
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
func (bdl *Bdatalog) ModifyTablePolicy(c *hst.Context) {
	checkMethodPost(c)
	var (
		request dto.ModifyTablePolicyReq
	)
	if err := parseJSONBody(c.R.Body, &request); err != nil {
		c.JSON2(http.StatusBadRequest, 1, err.Error())
	}

	if err := R.GetWatcher().ModifyPolicy(request.TableName, map[string]interface{}{
		"fields":      request.Fields,
		"out_date":    request.Outdate,
		"min_log_num": request.MinLogNum,
	}); err != nil {
		c.JSON2(http.StatusBadRequest, 2, err.Error())
	}

	c.JSON2(http.StatusOK, 0, "修改成功")
}
