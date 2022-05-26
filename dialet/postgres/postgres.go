package postgres

var (
	// 获取当前的所有数据表名
	sqlQueryTables = `
SELECT table_name
  FROM information_schema.tables
 WHERE table_schema='public'
   AND table_type='BASE TABLE'
`

	// 创建notify函数
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
	// 删除触发器
	sqlRemoveTrigger = `
DROP TRIGGER IF EXISTS pqstream_notify ON %s
`

	// 安装触发器
	sqlInstallTrigger = `
CREATE TRIGGER pqstream_notify
AFTER INSERT OR UPDATE OR DELETE ON %s
    FOR EACH ROW EXECUTE PROCEDURE pqstream_notify();
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

// Initial
func (p *PostgresDialet) Initial() error {
	// p.stream.installTrigger()
	return nil
}

func (p *PostgresDialet) Close() error {
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
func (p *PostgresDialet) Watch() chan interface{} {
	ch := make(chan interface{}, 100)
	for i := 0; i < 10; i++ {
		ch <- new(PostgresLog)
	}
	return ch
}
