package index

import (
	"bytes"
	"github.com/google/btree"
	"lovedb/data"
)

// Indexer 抽象索引接口（内存中），后续若想在内存中实现别的数据结构，直接实现这个接口就可以
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	// Size 索引中有多少条key
	Size() int

	// Iterator 返回索引迭代器
	Iterator(reverse bool) Iterator
}
type IndexerType = int8

const (
	// BTree Btree索引
	BTree IndexerType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewIndexer 根据用户传递的不同类型而实例化不同的内存数据结构
func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case BTree:
		return NewBtree()
	case ART:
		//todo
		return nil
	default:
		panic("unsupported index type")
	}
}

// Item 1.需要实现Btree中Item的方法less,才能作为接口传入方法中
// Item是树的每一个节点，只包含一个键值对
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 这个代表当前的item和传进来的另一个item进行比较的法则
func (ai *Item) Less(b btree.Item) bool {
	//Compare函数返回一个整数表示两个[]byte切片按字典序比较的结果（类同C的strcmp）。
	//如果a==b返回0；如果a<b返回-1；否则返回+1。nil参数视为空切片。
	return bytes.Compare(ai.key, b.(*Item).key) == -1 //根据写法，我们是根据每个kv键值对的key进行排序
}

// Iterator 通用的索引迭代器接口
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于(或小于)等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 当前索引是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
