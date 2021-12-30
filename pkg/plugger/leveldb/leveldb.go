package leveldb

import goleveldb "github.com/syndtr/goleveldb/leveldb"

type LevelDBDriver struct {
	dsn       string // 数据库地址 leveldb是文件数据库 传入文件地址
	initialed bool   // 标识是否初始化

	DB *goleveldb.DB
}

func NewLevelDBDriver(db string) *LevelDBDriver {
	return &LevelDBDriver{
		dsn: db,
	}
}

func (l *LevelDBDriver) Initial() error {
	if l.initialed {
		return nil
	}

	db, err := goleveldb.OpenFile(l.dsn, nil)
	if err != nil {
		return err
	}

	l.DB = db
	return nil
}

func (l *LevelDBDriver) Close() error {
	if l.DB == nil {
		return nil
	}

	l.DB.Close()
	return nil
}
