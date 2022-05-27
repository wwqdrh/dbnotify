package transport

type ITransport interface {
	Save(string)
	Load(string) ([]string, error)
}
