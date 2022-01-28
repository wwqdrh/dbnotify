package config

type AllConfig struct {
	DataLog      DataLog      `mapstructure:"data-log" yaml:"data-log"`
	PostgresConf PostgresConf `mapstructure:"postgres" yaml:"postgres"`
	LevelDBConf  LevelDBConf  `mapstructure:"leveldb" yaml:"leveldb"`
}
