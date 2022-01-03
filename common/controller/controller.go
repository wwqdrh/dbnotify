package controller

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"datamanager/common"
)

type (
	BaseResponse struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data"`
	}
)

// IndexView 主页面
func IndexView(c *gin.Context) {

}

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
func ListTable(c *gin.Context) {
	c.JSON(http.StatusOK, &ListTableResponse{
		Code: 0,
		Data: common.Mana.ListTable(),
	})
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
func ListHistoryByName(c *gin.Context) {
	// 去对应表的log中读取并返回整个json格式可以稍作处理
	var (
		request ListHistoryByNameRequest
		err     error
	)
	if err = c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": "参数错误",
		})
		return
	}

	logs, err := common.Mana.ListTableLog2(request.TableName, request.StartTime, request.EndTime, request.Page, request.PageSize)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  true,
		"message": "查询成功",
		"data":    logs,
	})
}

type (
	ListHistoryByFieldRequest struct {
		TableName string     `json:"table_name" binding:"required"`
		FieldName []string   `json:"field_name" binding:"required"`
		StartTime *time.Time `form:"start_time"`
		EndTime   *time.Time `form:"end_time"`
		Page      int        `json:"page"`
		PageSize  int        `json:"page_size"`
	}
)

// {ListHistoryByFIeld}
// @Summary
// @Description 根据字段名获取该字段在哪些情况下发生了变化，也就是需要将日志再读出来处理一下，查看哪些是修改了的，然后以字段作为id存储在leveldb中
// @Tags
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} loginResponse "ok"
// @Failure 400 {object} spec.Failure "we need username"
// @Router /api/login [post]
func ListHistoryByField(c *gin.Context) {
	var (
		request ListHistoryByFieldRequest
		err     error
	)
	if err = c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": "参数错误",
		})
		return
	}

	logs, err := common.Mana.ListTableByName2(request.TableName, request.FieldName, request.StartTime, request.EndTime, request.Page, request.PageSize)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  true,
		"message": "查询成功",
		"data":    logs,
	})
}

type (
	ModifyTableTimeRequest struct {
		TableName string `json:"table_name" binding:"required"`
		OutDate   int    `json:"out_date" binding:"required"`
	}
)

// {ModifyTableTime}
// @Summary
// @Description 配置表的过期时间单位天 如果与version配置不同 则需要重新创建触发器 相同则不需要修改
// @Tags 策略
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} BaseResponse "ok"
// @Router /table/policy/time [post]
func ModifyTableTime(c *gin.Context) {
	var (
		request ModifyTableTimeRequest
	)

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": "参数错误",
		})
		return
	}

	if err := common.Mana.ModifyTrigger(request.TableName, request.OutDate); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  true,
		"message": "修改成功",
	})
}

type (
	ModifyInsenseFieldRequest struct {
		TableName string   `json:"table_name" binding:"required"`
		Fields    []string `json:"fields" binding:"required"`
	}
)

// {ModifyInsenseField}
// @Summary
// @Description
// @Tags
// @Accept multipart/form-data
// @Produce json
// @Success 200 {object} loginResponse "ok"
// @Router /table/policy/field [post]
func ModifyInsenseField(c *gin.Context) {
	var (
		request ModifyInsenseFieldRequest
	)
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": "参数错误",
		})
		return
	}

	// 修改触发字段
	if err := common.Mana.ModifyField(request.TableName, request.Fields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": err.Error(),
		})
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{
		"status":  true,
		"message": "修改成功",
	})
}
