package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap 内存文件映射
type MMap struct {
	readerAT *mmap.ReaderAt
}

// NewMMapIOManager 初始化MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	//读取文件到虚拟内存空间中
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAT: readerAt}, nil

}

func (mmap *MMap) Read(b []byte, offSet int64) (int, error) {
	return mmap.readerAT.ReadAt(b, offSet)
}

func (mmap *MMap) Write(bytes []byte) (int, error) {
	panic("not implemented")
}

func (mmap *MMap) Sync() error {
	panic("not implemented")
}

func (mmap *MMap) Close() error {
	return mmap.readerAT.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAT.Len()), nil
}
