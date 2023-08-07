package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"lovedb/fio"
	"path/filepath"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

// DataFile 数据文件
// 嵌套用于IO读写的管理接口，由于是接口，后期可以接入别的例如mmap的io管理
type DataFile struct {
	FileId    uint32        //文件id
	WriteOff  int64         //文件写到了哪个位置
	IoManager fio.IoManager //用于数据读写的抽象接口，
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	// dirpath\000000001.data
	filename := GetDataFileName(dirPath, fileId)
	//初始化IO Manager文件管理接口，也就是打开了文件
	return newDataFile(filename, fileId, ioType)
}

// OpenHintFile 打开新的hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, HintFileName)
	return newDataFile(filename, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile  打开标识merge完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(filename, 0, fio.StandardFIO)
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	filename := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(filename, 0, fio.StandardFIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileID uint32, ioType fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileID,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

// Write 文件的写入
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	//更新文件中writeoff的字段
	df.WriteOff += int64(n)

	return nil
}

// WriteHintRecord 写入索引信息到hint文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encodeRecord, _ := EncodeLogRecord(record)
	return df.Write(encodeRecord)

}

// Sync 操作系统通常会使用缓存（如页面缓存）来提高性能，因此数据可能会暂时存储在内存中而未被写入硬盘，所以需要刷盘
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// ReadLogRecord 文件的读取,根据offset去文件中读取相应的logRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//先获取文件的大小
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	//如果该条记录是最后一条记录，且读取maxLogRecordHeaderSize超过文件大小了，就应该只读取到文件末尾
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	//读取header信息
	headerBuf, err := df.ReadNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	//对header的字节数组进行解码
	header, headerSize := DecodeLogRecordHeader(headerBuf)

	//下面两个条件代表读取到了文件的末尾，直接返回错误即可
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	LogRecord := &LogRecord{
		Type: header.recordType,
	}

	//取出keySize和valSize
	keySize, valSize := int64(header.keySize), int64(header.valueSize)
	//返回的记录长度就是headerSize+keySize+valSize
	recordSize := headerSize + keySize + valSize

	//根据size去读取用户实际的key和value
	if keySize > 0 || valSize > 0 {
		b1, err := df.ReadNBytes(keySize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		b2, err := df.ReadNBytes(valSize, offset+headerSize+keySize)
		if err != nil {
			return nil, 0, err
		}
		//fixme
		LogRecord.Key = b1
		LogRecord.Value = b2
	}

	//用crc校验数据的有效性
	crc := getLogRecordCRC(LogRecord, headerBuf[crc32.Size:headerSize]) //从crc后面开始到header结束
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return LogRecord, recordSize, nil
}

// SetIOManager 对当前文件设置我们的io方式
func (df *DataFile) SetIOManager(dirPath string, iotype fio.FileIOType) error {
	//将当前io方式关闭
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	//设置一个新的
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), iotype)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}

// ReadNBytes 调用io管理中的read方法读取字节
func (df *DataFile) ReadNBytes(n, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	if err != nil {
		return
	}
	return
}
