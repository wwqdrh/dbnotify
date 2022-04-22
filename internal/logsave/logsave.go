package logsave

type ILogSave interface {
	Write(interface{}) error
}
