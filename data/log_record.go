package data

type LogRecordByte = byte

const (
	LogRecordNormal LogRecordByte = iota
	LogRecordDeleted
)

// LogRecord 写入到数据文件的记录
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordByte
}

// LogRecordPos 内存索引的value值，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存放到磁盘的哪一个文件里
	Offset int64  //偏移，表示将数据存储到文件中的哪一个位置
}

// EncodeLogRecord 将logRecord转化为字节数组写入到文件中,并返回长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
