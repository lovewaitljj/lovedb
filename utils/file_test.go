package utils

import (
	"path/filepath"
	"testing"
)

func TestDirSize(t *testing.T) {
	dirsize, err := DirSize(filepath.Join("C:/Users/Lenovo/Desktop/helloworkld"))
	if err != nil {
		t.Log(err)
	}
	t.Log(dirsize)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	t.Log(err)
	t.Log(size / 1024 / 1024 / 1024)
}
