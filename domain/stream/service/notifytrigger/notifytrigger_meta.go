package notifytrigger

import (
	"sync"

	"github.com/wwqdrh/datamanager/runtime"
)

var R = runtime.Runtime

var MetaService *metaService = &metaService{}

var (
	sqlQueryTables = `
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema='public'
   AND table_type='BASE TABLE'
`
	sqlTriggerFunction = `
CREATE OR REPLACE FUNCTION pqstream_notify() RETURNS TRIGGER AS $$
    DECLARE 
        payload json;
        previous json;
        notification json;
    BEGIN
        IF (TG_OP = 'DELETE') THEN
            payload = row_to_json(OLD);
        ELSE
            payload = row_to_json(NEW);
        END IF;
        IF (TG_OP = 'UPDATE') THEN
            previous = row_to_json(OLD);
        END IF;
        
        notification = json_build_object(
                          'schema', TG_TABLE_SCHEMA,
                          'table', TG_TABLE_NAME,
                          'op', TG_OP,
						  'id', json_extract_path(payload, 'id')::text,
                          'payload', payload,
						  'previous', previous);
        IF (length(notification::text) >= 8000) THEN
          notification = json_build_object(
                          'schema', TG_TABLE_SCHEMA,
                          'table', TG_TABLE_NAME,
                          'op', TG_OP,
						  'id', json_extract_path(payload, 'id')::text,
						  'payload', payload);
        END IF;
        IF (length(notification::text) >= 8000) THEN
          notification = json_build_object(
                            'schema', TG_TABLE_SCHEMA,
                            'table', TG_TABLE_NAME,
                            'op', TG_OP,
							'id', json_extract_path(payload, 'id')::text);
        END IF;
        
        PERFORM pg_notify('pqstream_notify', notification::text);
        RETURN NULL; 
    END;
$$ LANGUAGE plpgsql;
`
	sqlRemoveTrigger = `
DROP TRIGGER IF EXISTS pqstream_notify ON %s
`
	sqlInstallTrigger = `
CREATE TRIGGER pqstream_notify
AFTER INSERT OR UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE PROCEDURE pqstream_notify();
`
	sqlFetchRowByID = `
	SELECT row_to_json(r)::text from (select * from %s where id = $1::%s) r;
`
)

type metaService struct {
	OutDate      int    // 过期时间 单位天
	MinLogNum    int    // 最少保留条数
	LogTableName string // 日志临时表的名字
	AllPolicy    *sync.Map
}

// Init 初始化metaService
func (s *metaService) Init() *metaService {
	if s.LogTableName == "" {
		s.LogTableName = R.GetConfig().TempLogTable
	}
	if s.OutDate <= 0 {
		s.OutDate = 15
	}
	s.AllPolicy = new(sync.Map)
	s.OutDate = R.GetConfig().Outdate
	s.MinLogNum = R.GetConfig().MinLogNum
	return s
}

// 创建触发器，包括dml通知类，以及ddl通知类
func (s *metaService) MustTrigger() {

}

// 接收请求到的操作信息，使用后端注册模式进行使用
func (s *metaService) Load() {

}
