package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func DestroyTmpFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIoManager(t *testing.T) {
	path := filepath.Join("..", "data", "a.data")
	fio, err := NewFileIoManager(path)
	defer DestroyTmpFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	fio.Close()

}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("../data/", "a.data")
	fio, err := NewFileIoManager(path)
	defer DestroyTmpFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
	fio.Close()

}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("../data/", "a.data")
	fio, err := NewFileIoManager(path)
	defer DestroyTmpFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)
	n, err := fio.Read(b, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)

	c := make([]byte, 5)
	n, err = fio.Read(c, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), c)
	fio.Close()
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("../data/", "a.data")
	fio, err := NewFileIoManager(path)
	defer DestroyTmpFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
	fio.Close()

}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("../data/", "a.data")
	fio, err := NewFileIoManager(path)
	defer DestroyTmpFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)

}
