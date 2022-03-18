package pglistener

import (
	"fmt"
	"testing"
)

func TestStart(t *testing.T) {
	queue := make(chan string)
	go Start(queue)
	for item := range queue {
		fmt.Println("dml:", item)
	}
}
