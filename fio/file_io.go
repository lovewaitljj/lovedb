package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File //系统文件描述符
}

// NewFileIoManager  初始化标准文件IO,打开文件
func NewFileIoManager(filename string) (*FileIO, error) {
	fd, err := os.OpenFile(
		filename,                          //路径加文件名
		os.O_CREATE|os.O_RDWR|os.O_APPEND, //没有会创建，可读可写，会追加写
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (f FileIO) Read(b []byte, offset int64) (int, error) {
	return f.fd.ReadAt(b, offset)
}

func (f FileIO) Write(b []byte) (int, error) {
	return f.fd.Write(b)
}

// Sync 记住要调用的是f中fd的sync和close方法，否则会产生无限递归调用
func (f FileIO) Sync() error {
	return f.fd.Sync()
}

func (f FileIO) Close() error {
	return f.fd.Close()
}

func (f FileIO) Size() (int64, error) {
	stat, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), err
}
