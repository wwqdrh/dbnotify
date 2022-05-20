package dialet

import (
	"fmt"
	"testing"

	"github.com/wwqdrh/datamanager/dialet/postgres"
)

func TestPostgresDialet(t *testing.T) {
	post := postgres.NewPostgresDialet(nil)
	for item := range post.Watch() {
		if val, ok := item.(ILogData); ok {
			fmt.Println(val.GetLabel())
		}
	}
}
