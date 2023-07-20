package data

import "lovedb/fio"

const DataFileNameSuffix = ".data"

// DataFile 数据文件
// 嵌套用于IO读写的管理接口，由于是接口，后期可以接入别的例如mmap的io管理
type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写到了哪个位置
	ioManager fio.IoManager //用于数据读写的抽象接口，
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {

	return nil, nil
}

// Write 文件的写入
func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 操作系统通常会使用缓存（如页面缓存）来提高性能，因此数据可能会暂时存储在内存中而未被写入硬盘，所以需要刷盘
func (df *DataFile) Sync() error {
	return nil
}

// ReadLogRecord 文件的读取
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//todo 存储的是byte数组，应该再解码取出LogRecord结构体返回,职能问题
	return nil, 0, nil
}
