package postgres

import (
	"fmt"
	"testing"
)

func TestUnmarshal(t *testing.T) {
	log, err := NewPostgresLog(`{"schema":"public","table":"notes","op":1,"id":"14","payload":{"created_at":null,"id":14,"name":"user1","note":"here is a sample note"}}`)
	if err != nil {
		t.Error(err)
	} else {
		fmt.Println(log.Payload)
	}
}
