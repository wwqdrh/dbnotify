//go:generate protoc -I . --go_out=. --go-grpc_out=. pqstream.proto
package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wwqdrh/logger"
)

var (
	// 获取当前的所有数据表名
	sqlQueryTables = `
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema='public'
   AND table_type='BASE TABLE'
`

	// 创建dml notify函数
	sqlTriggerFunction = `
CREATE EXTENSION IF NOT EXISTS hstore;
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
        PERFORM pg_notify('pqstream_notify', notification::text);
        RETURN NULL; 
    END;
$$ LANGUAGE plpgsql;
`

	// 创建ddl notify函数
	sqlDDLTriggerFunction = `
	CREATE EXTENSION IF NOT EXISTS hstore;
	CREATE OR REPLACE FUNCTION ddl_end_log_function() RETURNS event_trigger AS $$  
		DECLARE
			rec hstore;  
			notification json;
		BEGIN   
	  		select hstore(pg_stat_activity.*) into rec from pg_stat_activity where pid=pg_backend_pid();
			notification = json_build_object(
				'payload', json_build_object(
					 'query', rec->'query'
					)
				);
			PERFORM pg_notify('pqstream_notify', notification::text);
	 	END;  
	$$ LANGUAGE plpgsql;`

	// insert into %s ("table_name", "log", "action", "time")
	// values (SELECT
	// 	now(),
	// 	classid,
	// 	objid,
	// 	objsubid,
	// 	command_tag,
	// 	object_type,
	// 	schema_name,
	// 	object_identity,
	// 	in_extension
	// 	FROM pg_event_trigger_ddl_commands() left join select(rec,rec->'query',tg_tag,tg_event));
	// 删除触发器
	sqlRemoveTrigger = `
DROP TRIGGER IF EXISTS pqstream_notify ON %s
`

	sqlDDLRemoteTrigger = `
DROP TRIGGER IF EXISTS ddl_end_log_trigger
	`

	// 安装触发器
	sqlInstallTrigger = `
CREATE TRIGGER pqstream_notify
AFTER INSERT OR UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE PROCEDURE pqstream_notify();
`
	sqlDDLInstallTrigger = `
CREATE EVENT TRIGGER ddl_end_log_trigger
ON ddl_command_end when TAG IN ('CREATE TABLE', 'DROP TABLE', 'ALTER TABLE')
EXECUTE PROCEDURE ddl_end_log_function();
`

	// 根据rowid获取数据
	sqlFetchRowByID = `
	SELECT row_to_json(r)::text from (select * from %s where id = $1::%s) r;
`
)

type PostgresDialet struct {
	dsn    string
	stream *Stream
}

func NewPostgresDialet(dsn string) (*PostgresDialet, error) {
	stream, err := NewServer(dsn)
	if err != nil {
		return nil, err
	}

	return &PostgresDialet{
		dsn:    dsn,
		stream: stream,
	}, nil
}

func (p *PostgresDialet) Stream() *Stream {
	return p.stream
}

// Initial
func (p *PostgresDialet) Initial() error {
	// p.stream.installTrigger()
	if _, err := p.stream.db.Exec(sqlDDLTriggerFunction); err != nil {
		return err
	}
	// enable ddl
	if _, err := p.stream.db.Exec(sqlDDLInstallTrigger); err != nil {
		return err
	}
	return nil
}

func (p *PostgresDialet) Close() error {
	if _, err := p.stream.db.Exec(sqlDDLRemoteTrigger); err != nil {
		return err
	}
	return p.stream.Close()
}

// Register add policy for table
func (p *PostgresDialet) Register(table string) error {
	return p.stream.installTrigger(table)
}

func (p *PostgresDialet) UnRegister(table string) error {
	return p.stream.removeTrigger(table)
}

// 修改指定数据库数据表的日志存储策略
func (p *PostgresDialet) ModifyPolicy() error {
	return nil
}

// 查看指定数据库的日志策略
func (p *PostgresDialet) ListPolicy() error {
	return nil
}

// 删除某个指定策略
func (p *PostgresDialet) DeletePolicy() error {
	return nil
}

// 获取监听channel，能够获取当前的日志修改记录 日志记录格式需要
func (p *PostgresDialet) Watch(ctx context.Context) chan interface{} {
	res := make(chan interface{}, 8)

	q := make(chan string, 8)
	go func() {
		var r *PostgresLog
		for item := range q {
			if err := json.Unmarshal([]byte(item), &r); err != nil {
				fmt.Println(err)
				continue
			}
			res <- r
		}
	}()
	go func() {
		if err := p.stream.HandleEvents(ctx, q); err != nil {
			logger.DefaultLogger.Error(err.Error())
		}
	}()
	return res
}
