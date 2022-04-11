package dto

// ListHistoryByNameRequest 获取操作日志
type ListHistoryByNameReq struct {
	TableName string `form:"table_name" json:"table_name" binding:"required"`
	RecordID  string `form:"record_id" json:"record_id" binding:"required"`
	// StartTime *time.Time `form:"start_time" json:"start_time" time_format:"unix"`
	// EndTime   *time.Time `form:"end_time" json:"end_time" time_format:"unix"`
	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`
	Page      int   `json:"page"`
	PageSize  int   `json:"page_size"`
}

// ListHistoryAllReq 获取所有日志表
type ListHistoryAllReq struct {
	TableName string `form:"table_name" json:"table_name" binding:"required"`
	// StartTime *time.Time `form:"start_time" json:"start_time" time_format:"unix"`
	// EndTime   *time.Time `form:"end_time" json:"end_time" time_format:"unix"`
	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`
	Page      int   `json:"page"`
	PageSize  int   `json:"page_size"`
}

// RegisterTableReq 注册表
type RegisterTableReq struct {
	TableName   string   `json:"table_name" binding:"required"`
	SenseFields []string `json:"sense_fields" binding:"required"`
	MinLogNum   int      `json:"min_log_num" binding:"required"`
	Outdate     int      `json:"out_date" binding:"required"`
}

type UnRegisterTableReq struct {
	TableName string `json:"table_name" binding:"required"`
}

type ListTableFieldReq struct {
	TableName string `json:"table_name" binding:"required"`
}

// ModifyTablePolicyReq 修改策略
type ModifyTablePolicyReq struct {
	TableName string   `json:"table_name" binding:"required"`
	MinLogNum int      `json:"min_log_num"`
	Fields    []string `json:"fields"`
	Outdate   int      `json:"out_date"`
}
