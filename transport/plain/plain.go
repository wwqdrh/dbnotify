package plain

import (
	"errors"
	"fmt"
)

type PlainTransport struct {
}

func (p *PlainTransport) Save(log string) {
	fmt.Println(log)
}

func (p *PlainTransport) Load(string) ([]string, error) {
	return nil, errors.New("plain transport no implete this")
}
