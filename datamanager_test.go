package datamanager

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestAbs(t *testing.T) {
	fmt.Println(filepath.Abs("./"))
}
