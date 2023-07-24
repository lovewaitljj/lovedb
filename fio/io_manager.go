package fio

const DataFilePerm = 0644

// IoManager 抽象IO管理接口，可以接入不同的IO类型，目前支持标准文件IO
type IoManager interface {
	// Read 从给定的位置读取对应的数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中
	Write([]byte) (int, error)

	// Sync 内存缓冲区的数据持久化到硬盘当中
	Sync() error
	// Close 关闭文件
	Close() error

	// Size 获取到文件大小的方法
	Size() (int64, error)
}

// NewIOManager 初始化IO Manager
func NewIOManager(fileName string) (IoManager, error) {
	//目前只支持标准 FileIO
	return NewFileIoManager(fileName)
}
