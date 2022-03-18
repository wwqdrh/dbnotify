package datautil

import (
	"io/ioutil"
	"log"
	"os"
)

type TempFile struct {
	TmpName string
}

func (f *TempFile) NewFile(data string) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "tmp-")
	if err != nil {
		log.Fatal("Cannot create temporary file", err)
	}
	// Example writing to the file
	_, err = tmpFile.Write([]byte(data))
	if err != nil {
		log.Fatal("Failed to write to temporary file", err)
	}
	f.TmpName = tmpFile.Name()
}

func (f *TempFile) Close() {
	defer os.Remove(f.TmpName)
}
