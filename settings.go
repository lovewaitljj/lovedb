package lovedb

import "lovedb/index"

// 索引类型选择

type IndexerType uint8

const (
	// BTree Btree索引
	BTree IndexerType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B+树索引类型
	BPTree
)

// Options 配置文件，数据库启动，用户传递过去的配置信息
type Options struct {
	//数据库数据目录
	DirPath string

	//活跃文件的阈值
	DataFileSize int64

	//每次写数据是否持久化的配置项
	SyncWrite bool

	//累计的阈值直接进行自动持久化
	BytesPerSync uint

	//用户指定索引类型
	IndexType index.IndexerType

	//数据库启动时是否需要用mmap加载数据
	MMapAtStartUp bool
}

type IteratorOptions struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

// WriteBatchOption 批量写配置项
type WriteBatchOption struct {
	//一个批次最大的数据量
	MaxBatchNum uint
	//提交时是否持久化
	SyncWrites bool
}

var DefaultOptions = Options{
	DirPath:       "tmp",
	DataFileSize:  256 * 1024 * 1024, //256MB
	SyncWrite:     false,
	BytesPerSync:  0,
	IndexType:     index.BTree,
	MMapAtStartUp: true,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
