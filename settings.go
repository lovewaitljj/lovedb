package lovedb

import "lovedb/index"

// Options 配置文件，数据库启动，用户传递过去的配置信息
type Options struct {
	//数据库数据目录
	DirPath string

	//活跃文件的阈值
	DataFileSize int64

	//每次写数据是否持久化的配置项
	SyncWrite bool

	//用户指定索引类型
	IndexType index.IndexerType
}
