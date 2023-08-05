package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota

	// LogRecordDeleted 删除类型的记录
	LogRecordDeleted

	// LogRecordFinished 批量数据提交的fin标记记录
	LogRecordFinished
)

// crc type keySize valSize      （key和val 的size为变长元素）
//  4 +  1  +  5  +   5 = 15

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecordHeader LogRecord记录的头部信息
// todo 需要进行 Varint 编码，否则就是定长的uint32类型字段
type LogRecordHeader struct {
	crc        uint32        //crc校验值
	recordType LogRecordType //表示LogRecord的类型
	keySize    uint32        //key的长度
	valueSize  uint32        //value的长度
}

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 内存索引的value值，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存放到磁盘的哪一个文件里
	Offset int64  //偏移，表示将数据存储到文件中的哪一个位置
}

// TxRecord 暂存的事务相关的数据，当碰到fin字段，就将前面所有TxRecord更新到索引，需要记录type，key以及pos，所以组合在一起一个结构体
type TxRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 将logRecord转化为字节数组写入到文件中,并返回长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//先将header写入到字节数组中，crc先保留
	header := make([]byte, maxLogRecordHeaderSize)
	header[4] = logRecord.Type
	index := 5
	//5字节之后，存储的是key和value的size
	//使用变长类型，节省空间
	//做法是将logRecord中的key的长度进行变长的编码并放入到header的第五个字节往后
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	fmt.Println(index)
	//编码之后的实际长度！！第二个返回值
	size := index + len(logRecord.Key) + len(logRecord.Value)

	//定义返回的字节数组，第一个返回值
	resBytes := make([]byte, size)

	//将header部分拷贝过来
	copy(resBytes[:index], header[:index])

	//将后面的key和value实际值直接拷贝
	copy(resBytes[index:], logRecord.Key)
	copy(resBytes[index+len(logRecord.Key):], logRecord.Value)

	//对前四个字节以后所有取一个crc值
	crc := crc32.ChecksumIEEE(resBytes[4:])
	fmt.Println(crc)
	//PutUint32用于将无符号 32 位整数值编码为字节切片。
	binary.LittleEndian.PutUint32(resBytes[:4], crc)
	return resBytes, int64(size)
}

// EncodeLogRecordPos 将logRecordPos转化为字节数组写入到文件中,并返回
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	index := 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

// DecodeLogRecordPos 解码LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	index := 0
	fileId, n := binary.Varint(buf[:index])
	index += n
	offSet, n := binary.Varint(buf[:index])
	logRecordPos := &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offSet,
	}
	return logRecordPos
}

// DecodeLogRecordHeader 对头部信息的字节数组进行解码得到LogRecordHeader结构体记录,返回结构体和header的长度
// 由于key和value本身就是字节数组，所以不需要对整体进行解码
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	index := 5
	//Varint用来解码一个，仅仅一个变长的int值
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	//Varint用来解码一个，仅仅一个变长的int值
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)

}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	//再加上key和value继续取crc
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
